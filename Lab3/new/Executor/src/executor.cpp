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
#include <vector>
#include "file_iterator.h"
#include "page_iterator.h"
#include <sstream>
#include <algorithm>
#include <map>
#include "storage.h"
#include "exceptions/file_not_found_exception.h"

using namespace std;

namespace badgerdb {

void TableScanner::print() const {
    bufMgr->flushFile(&tableFile);
    string Tablename = tableSchema.getTableName();
    vector<DataType> dataType;
    cout<<"Table name: "<<Tablename<<"  ";
    vector<int> maxsize;
    for(int i = 0; i < tableSchema.getAttrCount(); i++){
        string attrname = tableSchema.getAttrName(i);
        cout<<"Attr"<<i + 1<<": "<<attrname<<" ";
        dataType.push_back(tableSchema.getAttrType(i));
        maxsize.push_back(tableSchema.getAttrMaxSize(i));
    }
    cout<<endl;
    badgerdb::File file = badgerdb::File::open(tableFile.filename());
    //print tuple
    // Iterate through all pages in the file.
    for (FileIterator iter = file.begin();
         iter != file.end();
         ++iter) {
      // Iterate through all records on the page.
        SlotId num = 0;
        Page p = *iter;
        PageId a = p.page_number();
        for(auto left_key : *iter){
            num++;
            const RecordId& record = {a, num};
            //string op
            int index = 8;//head
            string tup = /**page_iter;*/p.getRecord(record);
            string result = "";
            for(unsigned int j = 0; j < dataType.size(); j++){
                if(dataType[j] == 0){ //INT
                    string sub = tup.substr(index, 4);
                    char a1 = sub[0], a2 = sub[1], a3 = sub[2], a4 = sub[3];
                    int val = a1 << 24 | a2 << 16 | a3 << 8 | a4;
                    result.insert(result.size(), to_string(val));
                    result += " ";
                }
                else if(dataType[j] == 1){ //CHAR
                    string sub = tup.substr(index, maxsize[j]);
                    for(int k = 0; k < maxsize[j]; k++){
                        if(sub[k] != 0b00000000){
                            result += sub[k];
                        }
                    }
                    result += " ";
                }
                else if(dataType[j] == 2){ //VARCHAR
                    stringstream ss;
                    ss << tup[index];
                    ss >> maxsize[j];
                    string sub = tup.substr(index + 1, maxsize[j]);
                    for(int k = 0; k < maxsize[j]; k++){
                        result += sub[k];
                    }
                    result += " ";
                    maxsize[j]++;
                }
                if(maxsize[j] % 4 == 0)
                    index += maxsize[j];
                else
                    index += maxsize[j] + 4 - (maxsize[j] % 4);
            }
            std::cout << "Found record: " << result
                << " on page " << (*iter).page_number() << std::endl;
        }
    }
}

JoinOperator::JoinOperator(const File& leftTableFile,
                           const File& rightTableFile,
                           const TableSchema& leftTableSchema,
                           const TableSchema& rightTableSchema,
                           const Catalog* catalog,
                           BufMgr* bufMgr)
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
    const TableSchema& leftTableSchema,
    const TableSchema& rightTableSchema) {
    vector<Attribute> attrs;
    vector<string> attrname;
    // TODO: add attribute definitions
    //find the same attrs
    for(int i = 0; i < leftTableSchema.getAttrCount(); i++){
        attrname.push_back(leftTableSchema.getAttrName(i));
        Attribute* attr = new Attribute(leftTableSchema.getAttrName(i), leftTableSchema.getAttrType(i),
                                      leftTableSchema.getAttrMaxSize(i), leftTableSchema.isAttrNotNull(i),
                                      leftTableSchema.isAttrUnique(i));
        attrs.push_back(*attr);
    }
    for(int i = 0; i < rightTableSchema.getAttrCount(); i++){
        if(!count(attrname.begin(), attrname.end(), rightTableSchema.getAttrName(i))){
            Attribute* attr = new Attribute(rightTableSchema.getAttrName(i), rightTableSchema.getAttrType(i),
                                         rightTableSchema.getAttrMaxSize(i), rightTableSchema.isAttrNotNull(i),
                                         rightTableSchema.isAttrUnique(i));
            attrs.push_back(*attr);
        }
    }

  return TableSchema("TEMP_TABLE", attrs, true);
}

void JoinOperator::printRunningStats() const {
  cout << "# Result Tuples: " << numResultTuples << endl;
  cout << "# Used Buffer Pages: " << numUsedBufPages << endl;
  cout << "# I/Os: " << numIOs << endl;
}

bool OnePassJoinOperator::execute(int numAvailableBufPages, File& resultFile) {
    if (isComplete)
        return true;
    //leftTableFile = badgerdb::File::open("r.tbl");
    numResultTuples = 0;
    numUsedBufPages = 0;
    numIOs = 0;
    vector<string> attrname;
    vector<string> sameName;
    vector<Page> bufpage;
    map<string, vector<string>> hashMap;

    //find the same attrs
    for(int i = 0; i < leftTableSchema.getAttrCount(); i++){
        attrname.push_back(leftTableSchema.getAttrName(i));
    }
    for(int i = 0; i < rightTableSchema.getAttrCount(); i++){
        //vector<int>::iterator it = find(attrname.begin(), attrname.end(), rightTableSchema.getAttrName(i));
        if(count(attrname.begin(), attrname.end(), rightTableSchema.getAttrName(i))){
           sameName.push_back(rightTableSchema.getAttrName(i));
        }
    }
    //create a new file better
    //badgerdb::File right = badgerdb::File::open(catalog->getTableFilename(catalog->getTableId("s")));
    badgerdb::File right = badgerdb::File::open(catalog->getTableFilename(catalog->getTableId(rightTableSchema.getTableName())));
    try
    {
        File::remove("create.txt");
    }
    catch (FileNotFoundException e)
    {
    }
    badgerdb::File create = badgerdb::File::create("create.txt");
    //right first sort
    //badgerdb::File file = badgerdb::File::open(catalog->getTableFilename(catalog->getTableId("s")));
    badgerdb::File file = badgerdb::File::open(catalog->getTableFilename(catalog->getTableId(rightTableSchema.getTableName())));
    int count = 0;
    vector<TableSchema> tableSchema;
    tableSchema.push_back(rightTableSchema);
    tableSchema.push_back(leftTableSchema);
    while (count < 2){
        for (FileIterator iter = file.begin();
         iter != file.end();
         ++iter) {
      // Iterate through all records on the page.
            Page p = *iter;
            Page *new_page;
            PageId pagenum = p.page_number();
            bufMgr->readPage(&file, pagenum, new_page); // readPage first
            numUsedBufPages++;
            numIOs++;
            string hashString = "";
            for (PageIterator page_iter = p.begin();// read all tuples
                page_iter != p.end();
                ++page_iter) {
                string tup = *page_iter;
                string last = "";
                hashString = ""; //hash usage

                getHashString(hashString, sameName, tup, last, tableSchema[count]);

                if(count == 0){
                    if(hashMap.count(hashString) == 1){
                        hashMap[hashString].push_back(last);
                    }
                    else{
                        vector<string> stringList;
                        stringList.push_back(last);
                        hashMap.insert(pair<string, vector<string>> (hashString, stringList));
                    }
                    // read Buffer .....
                    bool flag = false;
                    Page *hashPage;
                    RecordId hashrecord;
                    for(unsigned int i = 0; i < bufpage.size(); i++){
                        bufMgr->readPage(&create, bufpage[i].page_number(), hashPage);
                        if(hashPage->hasSpaceForRecord(hashString)){
                            flag = true;
                            hashrecord = hashPage->insertRecord(hashString);
                            bufMgr->unPinPage(&create, bufpage[i].page_number(), true);
                            break;
                        }
                        bufMgr->unPinPage(&create, bufpage[i].page_number(), false);
                    }
                    if(!flag){
                        numUsedBufPages++;
                        PageId page_Id;
                        bufMgr->allocPage(&create, page_Id, hashPage);
                        hashrecord = hashPage->insertRecord(hashString);
                        bufMgr->unPinPage(&create, page_Id, true);
                        bufpage.push_back(*hashPage);
                    }
                }

                else{ //loop
                    if(hashMap.count(hashString) == 1){
                        vector<string> same = hashMap[hashString];
                        for(unsigned int i = 0; i < same.size(); i++){
                            numResultTuples++;
                            string temp = same[i].erase(0, 8);
                            string resultString = tup.insert(tup.size(), temp);
                            HeapFileManager::insertTuple(resultString, resultFile, bufMgr);
                        }
                    }
                }
            }
            bufMgr->unPinPage(&file, pagenum, false);
            bufMgr->flushFile(&file);
            numUsedBufPages--;
        }
        count++;
        //file = badgerdb::File::open(catalog->getTableFilename(catalog->getTableId("r")));
        file = badgerdb::File::open(catalog->getTableFilename(catalog->getTableId(leftTableSchema.getTableName())));
    }
    for(unsigned int i = 0; i < bufpage.size(); i++){
        bufMgr->disposePage(&create, bufpage[i].page_number());
    }
    bufMgr->flushFile(&resultFile);
    numUsedBufPages++;
    isComplete = true;
    return true;
}
void JoinOperator::getHashString(string& hashString, vector<string> sameName, string &tup, string& last /*contains the head*/,const TableSchema& tableSchema){
    last.insert(last.size(), tup);//copy
    unsigned int already_delete = 0;
    for(unsigned int i = 0; i < sameName.size(); i++){
        string comAttr = sameName[i];//ith common attribution
        unsigned int rank = tableSchema.getAttrNum(comAttr);
        int index = 8;//head
        for(unsigned int j = 0; j <= rank; j++){
            DataType dataType = tableSchema.getAttrType(j);
            if(dataType == 0){
                if(j == rank){
                    hashString.insert(hashString.size(), 1, tup[index]);
                    hashString.insert(hashString.size(), 1, tup[index + 1]);
                    hashString.insert(hashString.size(), 1, tup[index + 2]);
                    hashString.insert(hashString.size(), 1, tup[index + 3]);
                    last.erase(index - already_delete, 4);
                    already_delete += 4;
                }
                index += 4;
            }
            else if(dataType == 1){
                if(j == rank){
                    int k = 0;
                    for(k = 0; k < tableSchema.getAttrMaxSize(j); k++){
                        hashString += tup[index + k];
                    }
                    last.erase(index - already_delete, k);
                    already_delete += k;
                }
                index += tableSchema.getAttrMaxSize(j);
            }
            else if(dataType == 2){
                stringstream ss;
                ss << tup[index];
                int size  = 0;
                ss >> size;
                if (j == rank){
                    int k = 0;
                    for(k = 0; k < size; k++){
                        hashString += tup[index + 1 + k];
                    }
                    last.erase(index - already_delete, k + 1);
                    already_delete += k + 1;
                }
                index += size + 1;
            }
        }
        if(index % 4 != 0){
            last.erase(index - already_delete, (4 - (index % 4)));
            already_delete += 4 - (index % 4);
            index += 4 - (index % 4);

        }

    }
}

bool NestedLoopJoinOperator::execute(int numAvailableBufPages, File& resultFile) {
    if (isComplete)
        return true;

    numResultTuples = 0;
    numUsedBufPages = 0;
    numIOs = 0;
    vector<PageId> usedPage;// already used page
    vector<Page> already_in_buf;//using page
    vector<string> attrname;
    vector<string> sameName;
    map<string, vector<string>> hashMap;// recordid -> {hashstirng, last/*contains head*/}
    //find the same attrs
    for(int i = 0; i < leftTableSchema.getAttrCount(); i++){
        attrname.push_back(leftTableSchema.getAttrName(i));
    }
    for(int i = 0; i < rightTableSchema.getAttrCount(); i++){
        if(count(attrname.begin(), attrname.end(), rightTableSchema.getAttrName(i))){
           sameName.push_back(rightTableSchema.getAttrName(i));
        }
    }
    //first read min(M-1, page.size)'s rightTable
    //badgerdb::File leftfile = badgerdb::File::open(catalog->getTableFilename(catalog->getTableId("r")));
    //badgerdb::File rightfile = badgerdb::File::open(catalog->getTableFilename(catalog->getTableId("s")));
    badgerdb::File rightfile = badgerdb::File::open(catalog->getTableFilename(catalog->getTableId(rightTableSchema.getTableName())));
    badgerdb::File leftfile = badgerdb::File::open(catalog->getTableFilename(catalog->getTableId(leftTableSchema.getTableName())));
    int read_page_num = 0;
    int usedPageNum = 0;
    int sum =  0;
    for (FileIterator iter = rightfile.begin();
         iter != rightfile.end();
         ++iter){
        sum++;
    }
    while(usedPageNum < sum){
    for (FileIterator iter = rightfile.begin();
         iter != rightfile.end();
         ++iter){
        if(read_page_num >= numAvailableBufPages - 1)
            break;
        PageId pagenum = (*iter).page_number();
        //vector<int>::iterator iter=find(usedPage.begin(),usedPage.end(),pagenum);
        if(count(usedPage.begin(), usedPage.end(), pagenum)) //FOUND
            continue;
        //404 NOT Found
        Page *new_page;
        bufMgr->readPage(&rightfile, pagenum, new_page);
        for (PageIterator page_iter = (*new_page).begin();// read all tuples
                page_iter != (*new_page).end();
                ++page_iter){
            string righttuple = *page_iter;
            string last, hashString;
            getHashString(hashString, sameName, righttuple, last, rightTableSchema);

            if(hashMap.count(hashString) == 1){
                hashMap[hashString].push_back(last);
            }
            else{
                vector<string> stringList;
                stringList.push_back(last);
                hashMap.insert(pair<string, vector<string>> (hashString, stringList));
            }

        }
        usedPage.push_back(pagenum);
        already_in_buf.push_back(*new_page); // new or old?
        numIOs++;
        numUsedBufPages++;
        read_page_num++;
        usedPageNum += read_page_num;
    }
    for (FileIterator iter = leftfile.begin();
         iter != leftfile.end();
         ++iter){
        Page *left_new_page;
        Page p = *iter;
        PageId leftPagenum = p.page_number();
        bufMgr->readPage(&leftfile, leftPagenum, left_new_page);
        p = *left_new_page;
        numUsedBufPages++;
        numIOs++;
        for (PageIterator page_iter = p.begin();// read all tuples
                page_iter != p.end();
                ++page_iter){
            string lefttuple = *page_iter;
            string last, hashString;
            getHashString(hashString, sameName, lefttuple, last, leftTableSchema);
            if(hashMap.count(hashString) == 1){
                vector<string> same = hashMap[hashString];
                for(unsigned int i = 0; i < same.size(); i++){
                    numResultTuples++;
                    string temp = same[i].erase(0, 8);
                    string resultString = lefttuple.insert(lefttuple.size(), temp);
                    HeapFileManager::insertTuple(resultString, resultFile, bufMgr);
                }
            }
        }
        bufMgr->unPinPage(&leftfile, leftPagenum, false);
        numUsedBufPages--;
    }
    for(unsigned int i = 0; i < already_in_buf.size(); i++){
        bufMgr->unPinPage(&rightfile, already_in_buf[i].page_number(), false);
    }
    bufMgr->flushFile(&rightfile);
    already_in_buf.clear();
    read_page_num = 0;
    }
    numUsedBufPages++;
    isComplete = true;
    return true;
}

BucketId GraceHashJoinOperator::hash(const string& key) const {
  std::hash<string> strHash;
  return strHash(key) % numBuckets;
}

bool GraceHashJoinOperator ::execute(int numAvailableBufPages, File& resultFile) {
    if (isComplete)
        return true;
    //leftTableFile = badgerdb::File::open("r.tbl");
    numResultTuples = 0;
    numUsedBufPages = 0;
    numIOs = 0;
    vector<string> attrname;
    vector<string> sameName;
    vector<Page> right_bufpage;
    vector<Page> left_bufpage;
    numBuckets = numAvailableBufPages - 1;
    vector<File> leftFile;
    vector<File> rightFile;
    //map<string, string>

    //find the same attrs
    for(int i = 0; i < leftTableSchema.getAttrCount(); i++){
        attrname.push_back(leftTableSchema.getAttrName(i));
    }
    for(int i = 0; i < rightTableSchema.getAttrCount(); i++){
        //vector<int>::iterator it = find(attrname.begin(), attrname.end(), rightTableSchema.getAttrName(i));
        if(count(attrname.begin(), attrname.end(), rightTableSchema.getAttrName(i))){
           sameName.push_back(rightTableSchema.getAttrName(i));
        }
    }
    //create a new file better
    badgerdb::File right = badgerdb::File::open(catalog->getTableFilename(catalog->getTableId("s")));

    for(int i = 0; i < numAvailableBufPages - 1; i++){ //0-99
        PageId pageId;
        Page *page;
        try
        {
            File::remove("create"+ to_string(i));
        }
        catch (FileNotFoundException e)
        {
        }
        badgerdb::File *create = new File("create"+ to_string(i), true);// create right page for every runs
        //badgerdb::File create1 = badgerdb::File::create("create"+ to_string(i + 2));

        //create = File::open("create"+ to_string(i));
        bufMgr->allocPage(create, pageId, page);
        right_bufpage.push_back(*page);
        rightFile.push_back(*create);
        numUsedBufPages++;
    }
    //right first sort
    badgerdb::File file = badgerdb::File::open(catalog->getTableFilename(catalog->getTableId("s")));
    int count = 0;
    vector<TableSchema> tableSchema;
    tableSchema.push_back(rightTableSchema);
    tableSchema.push_back(leftTableSchema);
    while (count < 2){
        for (FileIterator iter = file.begin();
         iter != file.end();
         ++iter) {
      // Iterate through all records on the page.
            Page p = *iter;
            Page *new_page;
            PageId pagenum = p.page_number();
            bufMgr->readPage(&file, pagenum, new_page); // readPage first
            numUsedBufPages++;
            numIOs++;
            string hashString = "";
            for (PageIterator page_iter = p.begin();// read all tuples
                page_iter != p.end();
                ++page_iter) {
                string tup = *page_iter;
                string last = "";
                hashString = ""; //hash usage
                //get hash string
                getHashString(hashString, sameName, tup, last, tableSchema[count]);
                //calculate hash number
                BucketId numID = GraceHashJoinOperator::hash(hashString);
                if(count == 0){
                    right_bufpage[numID].insertRecord(tup);
                    rightFile[numID].writePage(right_bufpage[numID]);
                }
                else{
                    left_bufpage[numID].insertRecord(tup);
                    leftFile[numID].writePage(left_bufpage[numID]);
                }
            }
            bufMgr->unPinPage(&file, pagenum, false);
            bufMgr->flushFile(&file);
            numUsedBufPages--;
        }
        if(count == 0)
            for(int i = 0; i < numBuckets; i++){

                bufMgr->unPinPage(&rightFile[i], right_bufpage[i].page_number(), true);
                bufMgr->flushFile(&rightFile[i]);
                numIOs++;
                numUsedBufPages--;
            }
        else
            for(int i = 0; i < numBuckets; i++){

                //File currentFile = leftFile[i];
                bufMgr->unPinPage(&leftFile[i], left_bufpage[i].page_number(), true);
                bufMgr->flushFile(&leftFile[i]);
                numIOs++;
            }
        count++;
        file = badgerdb::File::open(catalog->getTableFilename(catalog->getTableId("r")));
        if(count == 1)
            for(int i = 0; i < numBuckets; i++){ //0-99
                PageId pageId;
                Page *page;
                try
                {
                    File::remove("save"+ to_string(i));
                }
                catch (FileNotFoundException e)
                {
                }
                badgerdb::File *save = new File("save"+to_string(i), true);// create right page for every runs
                bufMgr->allocPage(save, pageId, page);
                left_bufpage.push_back(*page);
                leftFile.push_back(*save);
                numUsedBufPages++;
            }
    }
    //find the same attrs
    vector<Attribute> leftattr, rightattr;
    for(int i = 0; i < leftTableSchema.getAttrCount(); i++){
        Attribute* attr = new Attribute(leftTableSchema.getAttrName(i), leftTableSchema.getAttrType(i),
                                      leftTableSchema.getAttrMaxSize(i), leftTableSchema.isAttrNotNull(i),
                                      leftTableSchema.isAttrUnique(i));
        leftattr.push_back(*attr);
    }
    for(int i = 0; i < rightTableSchema.getAttrCount(); i++){
        Attribute* attr = new Attribute(rightTableSchema.getAttrName(i), rightTableSchema.getAttrType(i),
                                         rightTableSchema.getAttrMaxSize(i), rightTableSchema.isAttrNotNull(i),
                                         rightTableSchema.isAttrUnique(i));
        rightattr.push_back(*attr);
    }
    Catalog c = Catalog("Grace");
    try
                {
                    File::remove("leftFile");
                    File::remove("rightFile");
                }
                catch (FileNotFoundException e)
                {
                }
    File left_file = File::create("leftFile");
    File right_file = File::create("rightFile");
    for(int i = 0; i < numBuckets; i++){
        TableSchema left = TableSchema("lefttable"+to_string(i), leftattr, true); // false
        TableSchema right = TableSchema("righttable"+to_string(i), rightattr, true);
        c.addTableSchema(left, "save"+to_string(i));
        c.addTableSchema(right, "create"+to_string(i));
        OnePassJoinOperator joinOperator(
        left_file, right_file, left, right, &c, bufMgr);
        joinOperator.execute(numAvailableBufPages, resultFile);
        //File::remove("save" + to_string(i));
        //File::remove("create" + to_string(i));
    }
    //File::remove("leftFile");
    //File::remove("rightFile");
    File refile = badgerdb::File::open(resultFile.filename());
    for (FileIterator iter = refile.begin();
         iter != refile.end();
         ++iter) {
        for(auto left_key : *iter){
                numResultTuples ++;
        }
    }

    numUsedBufPages++;
    isComplete = true;
    return true;

}

}  // namespace badgerdb
