/**
 * @author Zhaonian Zou <znzou@hit.edu.cn>,
 * School of Computer Science and Technology,
 * Harbin Institute of Technology, China
 */

#include "executor.h"

#include <functional>
#include <string>
#include <iostream>
#include <ctime>
#include <cmath>
#include <utility>
#include <exceptions/buffer_exceeded_exception.h>

#include "storage.h"
#include "file_iterator.h"
#include "page_iterator.h"

using namespace std;

namespace badgerdb {

    void TableScanner::print() const {
        // TODO: Printer the contents of the table
        badgerdb::File file = badgerdb::File::open(tableFile.filename());
        for (badgerdb::FileIterator iter = file.begin();
             iter != file.end();
             ++iter) {
            badgerdb::Page page = *iter;
            badgerdb::Page *buffered_page;
            bufMgr->readPage(&file, page.page_number(), buffered_page);

            for (badgerdb::PageIterator page_iter = buffered_page->begin();
                 page_iter != buffered_page->end();
                 ++page_iter) {
                string key = *page_iter;
                string print_key = "(";
                int current_index = 0;
                for (int i = 0; i < tableSchema.getAttrCount(); ++i) {
                    switch (tableSchema.getAttrType(i)) {
                        case INT: {
                            int true_value = 0;
                            for (int j = 0; j < 4; ++j) {
                                if (std::string(key, current_index + j, 1)[0] == '\0') {
                                    continue;  // \0 is actually representing 0
                                }
                                true_value += (std::string(key, current_index + j, 1))[0] * pow(128, 3 - j);
                            }
                            print_key += to_string(true_value);
                            current_index += 4;
                            break;
                        }
                        case CHAR: {
                            int max_len = tableSchema.getAttrMaxSize(i);
                            print_key += std::string(key, current_index, max_len);
                            current_index += max_len;
                            current_index += (4 - (max_len % 4)) % 4;//align to the multiple of 4
                            break;
                        }
                        case VARCHAR: {
                            int actual_len = key[current_index];
                            current_index++;
                            print_key += std::string(key, current_index, actual_len);
                            current_index += actual_len;
                            current_index += (4 - ((actual_len + 1) % 4)) % 4;//align to the multiple of 4
                            break;
                        }
                    }
                    print_key += ",";
                }
                print_key[print_key.size() - 1] = ')';  // change the last ',' to ')'
                cout << print_key << endl;
            }
            bufMgr->unPinPage(&file, page.page_number(), false);
        }
        bufMgr->flushFile(&file);
    }

    JoinOperator::JoinOperator(const File &leftTableFile,
                               const File &rightTableFile,
                               const TableSchema &leftTableSchema,
                               const TableSchema &rightTableSchema,
                               const Catalog *catalog,
                               BufMgr *bufMgr)
            : leftTableFile(leftTableFile),
              rightTableFile(rightTableFile),
              leftTableSchema(leftTableSchema),
              rightTableSchema(rightTableSchema),
              resultTableSchema(
                      createResultTableSchema(leftTableSchema, rightTableSchema)),
              catalog(catalog),
              bufMgr(bufMgr),
              isComplete(false) {
        // nothing
    }

    TableSchema JoinOperator::createResultTableSchema(
            const TableSchema &leftTableSchema,
            const TableSchema &rightTableSchema) {
        vector<Attribute> attrs;

        // first add all the left table attrs to the result table
        for (int k = 0; k < leftTableSchema.getAttrCount(); ++k) {
            Attribute new_attr = Attribute(leftTableSchema.getAttrName(k), leftTableSchema.getAttrType(k),
                                           leftTableSchema.getAttrMaxSize(k), leftTableSchema.isAttrNotNull(k),
                                           leftTableSchema.isAttrUnique(k));
            attrs.push_back(new_attr);
        }

        // test every right table attrs, if it doesn't have the same attr(name and type) in the left table,
        // then add it to the result table
        for (int i = 0; i < rightTableSchema.getAttrCount(); ++i) {
            bool has_same = false;
            for (int j = 0; j < leftTableSchema.getAttrCount(); ++j) {
                if ((leftTableSchema.getAttrType(j) == rightTableSchema.getAttrType(i)) &&
                    (leftTableSchema.getAttrName(j) == rightTableSchema.getAttrName(i))) {
                    has_same = true;
                }
            }
            if (!has_same) {
                Attribute new_attr = Attribute(rightTableSchema.getAttrName(i), rightTableSchema.getAttrType(i),
                                               rightTableSchema.getAttrMaxSize(i), rightTableSchema.isAttrNotNull(i),
                                               rightTableSchema.isAttrUnique(i));
                attrs.push_back(new_attr);
            }
        }
        return TableSchema("TEMP_TABLE", attrs, true);
    }

    void JoinOperator::printRunningStats() const {
        cout << "# Result Tuples: " << numResultTuples << endl;
        cout << "# Used Buffer Pages: " << numUsedBufPages << endl;
        cout << "# I/Os: " << numIOs << endl;
    }

    /**
     * find the common attributes of two schemas, used to do the natural join
     * @param leftTableSchema
     * @param rightTableSchema
     * @return
     */
    vector<Attribute> get_common_attributes(const TableSchema &leftTableSchema, const TableSchema &rightTableSchema) {
        vector<Attribute> common_attrs;
        for (int i = 0; i < rightTableSchema.getAttrCount(); ++i) {
            for (int j = 0; j < leftTableSchema.getAttrCount(); ++j) {
                if ((leftTableSchema.getAttrType(j) == rightTableSchema.getAttrType(i)) &&
                    (leftTableSchema.getAttrName(j) == rightTableSchema.getAttrName(i))) {
                    Attribute new_attr = Attribute(rightTableSchema.getAttrName(i), rightTableSchema.getAttrType(i),
                                                   rightTableSchema.getAttrMaxSize(i),
                                                   rightTableSchema.isAttrNotNull(i),
                                                   rightTableSchema.isAttrUnique(i));
                    common_attrs.push_back(new_attr);
                }
            }
        }
        return common_attrs;
    }

    /**
     * use the original key to generate the search key
     * @param key
     * @param common_attrs
     * @param TableSchema
     * @return
     */
    string construct_search_key(string key, vector<Attribute> &common_attrs, const TableSchema &TableSchema) {
        string search_key;
        int current_index = 0;
        int current_attr_index = 0;
        for (int i = 0; i < TableSchema.getAttrCount(); ++i) {
            switch (TableSchema.getAttrType(i)) {
                case INT: {
                    if (TableSchema.getAttrName(i) == common_attrs[current_attr_index].attrName
                        && TableSchema.getAttrType(i) == common_attrs[current_attr_index].attrType) {
                        search_key += std::string(key, current_index, 4);
                        current_attr_index++;
                    }
                    current_index += 4;
                    break;
                }
                case CHAR: {
                    int max_len = TableSchema.getAttrMaxSize(i);
                    if (TableSchema.getAttrName(i) == common_attrs[current_attr_index].attrName
                        && TableSchema.getAttrType(i) == common_attrs[current_attr_index].attrType) {
                        search_key += std::string(key, current_index, max_len);
                        current_attr_index++;
                    }
                    current_index += max_len;
                    current_index += (4 - (max_len % 4)) % 4;;//align to the multiple of 4
                    break;
                }
                case VARCHAR: {
                    int actual_len = key[current_index];
                    current_index++;
                    if (TableSchema.getAttrName(i) == common_attrs[current_attr_index].attrName
                        && TableSchema.getAttrType(i) == common_attrs[current_attr_index].attrType) {
                        search_key += std::string(key, current_index, actual_len);
                        current_attr_index++;
                    }
                    current_index += actual_len;
                    current_index += (4 - ((actual_len + 1) % 4)) % 4;//align to the multiple of 4
                    break;
                }
            }
            if (current_attr_index >= common_attrs.size())
                break;
        }
        return search_key;
    }

    /**
     * construct the final tuple by natural joining two tuples
     * @param left_key left tuple
     * @param right_key right tuple
     * @param leftTableSchema
     * @param rightTableSchema
     * @return
     */
    string construct_result_tuple(string left_key, string right_key, const TableSchema &leftTableSchema,
                                  const TableSchema &rightTableSchema) {
        int cur_right_index = 0; //current substring index in the right table key
        string result_tuple = left_key;

        for (int i = 0; i < rightTableSchema.getAttrCount(); ++i) {
            bool has_same = false;
            for (int j = 0; j < leftTableSchema.getAttrCount(); ++j) {
                if ((leftTableSchema.getAttrType(j) == rightTableSchema.getAttrType(i)) &&
                    (leftTableSchema.getAttrName(j) == rightTableSchema.getAttrName(i))) {
                    has_same = true;
                }
            }
            //if the key is only owned by right table, add it to the result tuple
            switch (rightTableSchema.getAttrType(i)) {
                case INT: {
                    if (!has_same) {
                        result_tuple += std::string(right_key, cur_right_index, 4);
                    }
                    cur_right_index += 4;
                    break;
                }
                case CHAR: {
                    int max_len = rightTableSchema.getAttrMaxSize(i);
                    if (!has_same) {
                        result_tuple += std::string(right_key, cur_right_index, max_len);
                    }
                    cur_right_index += max_len;
                    unsigned align_ = (4 - (max_len % 4)) % 4;//align to the multiple of 4
                    for (int k = 0; k < align_; ++k) {
                        result_tuple += "0";
                        cur_right_index++;
                    }
                    break;
                }
                case VARCHAR: {
                    int actual_len = right_key[cur_right_index];
                    result_tuple += std::string(right_key, cur_right_index, 1);
                    cur_right_index++;
                    if (!has_same) {
                        result_tuple += std::string(right_key, cur_right_index, actual_len);
                    }
                    cur_right_index += actual_len;
                    unsigned align_ = (4 - ((actual_len + 1) % 4)) % 4;//align to the multiple of 4
                    for (int k = 0; k < align_; ++k) {
                        result_tuple += "0";
                        cur_right_index++;
                    }
                    break;
                }
            }
        }
        return result_tuple;
    }

    bool OnePassJoinOperator::execute(int numAvailableBufPages, File &resultFile) {
        if (isComplete)
            return true;

        numResultTuples = 0;
        numUsedBufPages = 0;
        numIOs = 0;

        // TODO: Execute the join algorithm
        vector<PageId> buffered_right_pages;
        // first find the common attributes
        vector<Attribute> common_attrs = get_common_attributes(leftTableSchema, rightTableSchema);
        // hash the common value to the RecordId
        map<string, RecordId> onePassHash;
        //iterator all the pages and load them into the buffer
        badgerdb::File right_file = badgerdb::File::open(rightTableFile.filename());

        /*
         * read the right table into hash
         */
        for (badgerdb::FileIterator iter = right_file.begin();
             iter != right_file.end();
             ++iter) {
            badgerdb::Page page = *iter;
            badgerdb::Page *buffered_page;
            bufMgr->readPage(&right_file, page.page_number(), buffered_page);
            buffered_right_pages.push_back(page.page_number());
            numIOs++;
            numUsedBufPages++;

            SlotId current_slot = Page::INVALID_SLOT; // the SLOT before the first slot
            for (badgerdb::PageIterator record_iter = buffered_page->begin();
                 record_iter != buffered_page->end();
                 ++record_iter) {
                // synchronized iterator the slots
                current_slot = record_iter.getNextUsedSlot(current_slot); // get the first slot in the page
                RecordId current_recordId = {buffered_page->page_number(), current_slot};
                string key = *record_iter;
                // construct the search key（从原始键构建查找键）
                string search_key = construct_search_key(key, common_attrs, rightTableSchema);
                onePassHash[search_key] = current_recordId;
            }
        }

        badgerdb::File left_file = badgerdb::File::open(leftTableFile.filename());

        // only use one left table page at the same time (the character of one-pass join)
        numUsedBufPages++;
        /*
         * do the final join
         */
        // pages
        for (badgerdb::FileIterator iter = left_file.begin();
             iter != left_file.end();
             ++iter) {
            badgerdb::Page page = *iter;
            badgerdb::Page *buffered_page;
            bufMgr->readPage(&left_file, page.page_number(), buffered_page);

            numIOs++;
            // records
            for (auto left_key : *buffered_page) {
                string result_tuple;
                // construct the search key（从原始键构建查找键）
                string search_key = construct_search_key(left_key, common_attrs, leftTableSchema);

                if (onePassHash.find(search_key) != onePassHash.end()) {  // has found the correspond key
                    badgerdb::Page *right_page;

                    // don't need to add I/O nums
                    // because right file has already been read into hash
                    bufMgr->readPage(&right_file, onePassHash[search_key].page_number, right_page);
                    string right_key = right_page->getRecord(onePassHash[search_key]);

                    // construct the final result tuple
                    result_tuple = construct_result_tuple(left_key, right_key, leftTableSchema, rightTableSchema);

                    bufMgr->unPinPage(&right_file, onePassHash[search_key].page_number, false);
                    HeapFileManager::insertTuple(result_tuple, resultFile, bufMgr);
                    numResultTuples++;
                }


            }
            // unpin the left page
            bufMgr->unPinPage(&left_file, buffered_page->page_number(), false);
            bufMgr->flushFile(&left_file);
        }
        // unpin the all the right page that read into the buffer
        for (unsigned int buffered_right_page : buffered_right_pages) {
            bufMgr->unPinPage(&right_file, buffered_right_page, false);
        }
        bufMgr->flushFile(&right_file);
        isComplete = true;
        return true;
    }

    bool NestedLoopJoinOperator::execute(int numAvailableBufPages, File &resultFile) {
        if (isComplete)
            return true;

        numResultTuples = 0;
        numUsedBufPages = 0;
        numIOs = 0;

        // TODO: Execute the join algorithm
        // first find the common attributes
        vector<Attribute> common_attrs = get_common_attributes(leftTableSchema, rightTableSchema);
        //iterator all the pages and load them into the buffer
        badgerdb::File left_file = badgerdb::File::open(leftTableFile.filename());
        badgerdb::File right_file = badgerdb::File::open(rightTableFile.filename());

        //register how many pages had been buffered, to control the buffered the page num less than M - 1
        vector<PageId> has_buffered_page;
        //register those pages that had not been read
        vector<PageId> wait_left_pages;

        /*
         * read the left table
         */
        for (auto left_page : left_file) {
            wait_left_pages.push_back(left_page.page_number());
        }

        while (true) {
            while (!wait_left_pages.empty()) {
                PageId left_pageId = wait_left_pages[wait_left_pages.size() - 1];
                wait_left_pages.pop_back();

                badgerdb::Page *buffered_left_page;
                // read the left file until buffered page num reached M - 1
                bufMgr->readPage(&left_file, left_pageId, buffered_left_page);
                has_buffered_page.push_back(left_pageId);
                bufMgr->unPinPage(&left_file, left_pageId, false);

                numIOs++;
                numUsedBufPages++;

                if (has_buffered_page.size() > (numAvailableBufPages - 2))
                    break;
            }
            /*
             * read one right page into buffer and do the loop join
             */
            for (auto right_page : right_file) {
                badgerdb::Page *buffered_right_page;
                bufMgr->readPage(&right_file, right_page.page_number(), buffered_right_page);
                numIOs++;
                numUsedBufPages++;
                for (unsigned int left_pageId : has_buffered_page) {
                    badgerdb::Page *buffered_left_page;
                    // no need to increment the I/O because the page is in the file
                    bufMgr->readPage(&left_file, left_pageId, buffered_left_page);
                    for (auto left_key : *buffered_left_page) {
                        for (auto right_key : *buffered_right_page) {
                            string left_search_key = construct_search_key(left_key, common_attrs, leftTableSchema);
                            string right_search_key = construct_search_key(right_key, common_attrs, rightTableSchema);
                            if (left_search_key == right_search_key) {
                                //can do natural join between these two keys
                                string result_tuple = construct_result_tuple(left_key, right_key, leftTableSchema,
                                                                             rightTableSchema);
                                HeapFileManager::insertTuple(result_tuple, resultFile, bufMgr);
                                numResultTuples++;
                            }
                        }
                    }
                    bufMgr->unPinPage(&left_file, left_pageId, false);
                }
                bufMgr->unPinPage(&right_file, right_page.page_number(), false);
                bufMgr->flushFile(&right_file);
            }
            bufMgr->flushFile(&left_file);
            has_buffered_page.clear();
            if (wait_left_pages.empty())
                break;
        }
        isComplete = true;
        return true;
    }

    BucketId GraceHashJoinOperator::hash(const string &key) const {
        std::hash<string> strHash;
        return strHash(key) % numBuckets;
    }

    bool GraceHashJoinOperator::execute(int numAvailableBufPages, File &resultFile) {
        if (isComplete)
            return true;

        numResultTuples = 0;
        numUsedBufPages = 0;
        numIOs = 0;

        // TODO: Execute the join algorithm
        // the bucket number is equal to M - 1
        numBuckets = numAvailableBufPages - 1;

        // first find the common attributes
        vector<Attribute> common_attrs = get_common_attributes(leftTableSchema, rightTableSchema);
        //iterator all the pages and load them into the buffer
        badgerdb::File left_file = badgerdb::File::open(leftTableFile.filename());
        badgerdb::File right_file = badgerdb::File::open(rightTableFile.filename());
        // hash the bucket to the RecordId that belong to the left file
        map<int, vector<RecordId>> left_GraceHash;
        // hash the bucket to the RecordId that belong to the right file
        map<int, vector<RecordId>> right_GraceHash;

        /*
         * do the hash bucket for the left file (左表hash分桶)
         */
        // create the bucket file used to store pages
        string leftBucketFilename = "left_bucket.tbl";
        File leftBucketFile = File::create(leftBucketFilename);
        for (int i = 0; i < numBuckets; ++i) {
            PageId pageNo;
            Page *page;
            bufMgr->allocPage(&leftBucketFile,pageNo,page);
            numUsedBufPages++;
            bufMgr->unPinPage(&leftBucketFile,pageNo, false);
        }
        numUsedBufPages++;
        for (auto left_page : left_file) {
            badgerdb::Page *buffered_left_page;
            bufMgr->readPage(&left_file, left_page.page_number(), buffered_left_page);
            numIOs+=2;


            for (auto left_key : *buffered_left_page) {
                int hash_value = hash(construct_search_key(left_key,common_attrs,leftTableSchema))+1;
                Page *bucket_page;
                bufMgr->readPage(&leftBucketFile,hash_value,bucket_page);
                bucket_page->insertRecord(left_key);
                bufMgr->unPinPage(&leftBucketFile,hash_value, true);
                bufMgr->flushFile(&leftBucketFile);
            }
            bufMgr->unPinPage(&left_file,left_page.page_number(), false);
            numIOs++;
        }

        /*
         * do the hash bucket for the right file (右表hash分桶)
         */
        string rightBucketFilename = "right_bucket.tbl";
        File rightBucketFile = File::create(rightBucketFilename);
        for (int i = 0; i < numBuckets; ++i) {
            PageId pageNo;
            Page *page;
            bufMgr->allocPage(&rightBucketFile,pageNo,page);
            bufMgr->unPinPage(&rightBucketFile,pageNo, false);
        }
        for (auto right_page : right_file) {
            badgerdb::Page *buffered_right_page;
            bufMgr->readPage(&right_file, right_page.page_number(), buffered_right_page);
            numIOs+=2;

            for (auto right_key : *buffered_right_page) {
                int hash_value = hash(construct_search_key(right_key,common_attrs,rightTableSchema)) + 1;
                Page *bucket_page;
                bufMgr->readPage(&rightBucketFile,hash_value,bucket_page);
                bucket_page->insertRecord(right_key);
                bufMgr->unPinPage(&rightBucketFile,hash_value, true);
                bufMgr->flushFile(&rightBucketFile);
            }
            bufMgr->unPinPage(&right_file,right_page.page_number(), false);
            numIOs++;
        }

        /*
         * do the final hash join
         */
        for (int j = 1; j <= numBuckets; ++j) {
            Page *buffered_left_page;
            bufMgr->readPage(&leftBucketFile,j,buffered_left_page);
            Page *buffered_right_page;
            bufMgr->readPage(&rightBucketFile,j,buffered_right_page);
            for (auto left_key : *buffered_left_page) {
                for (auto right_key : *buffered_right_page) {
                    string left_search_key = construct_search_key(left_key, common_attrs, leftTableSchema);
                    string right_search_key = construct_search_key(right_key, common_attrs, rightTableSchema);
                    if (left_search_key == right_search_key) {
                        //can do natural join between these two keys
                        string result_tuple = construct_result_tuple(left_key, right_key, leftTableSchema,
                                                                     rightTableSchema);
                        HeapFileManager::insertTuple(result_tuple, resultFile, bufMgr);
                        numResultTuples++;
                    }
                }
            }
            bufMgr->unPinPage(&leftBucketFile,j, false);
            bufMgr->unPinPage(&rightBucketFile,j, false);
        }

        isComplete = true;
        return true;
    }

}  // namespace badgerdb