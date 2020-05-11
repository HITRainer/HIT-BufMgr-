/**
 * @author Zhaonian Zou <znzou@hit.edu.cn>,
 * School of Computer Science and Technology,
 * Harbin Institute of Technology, China
 */

#include "storage.h"
#include <vector>
#include <regex>
#include "file_iterator.h"

using namespace std;

namespace badgerdb {

RecordId HeapFileManager::insertTuple(const string& tuple,
                                      File& file,
                                      BufMgr* bufMgr) {
    RecordId record; //iterator
    bool flag = false;
    // Iterate through all pages in the file.
    Page *pagepoint;
    PageId pageNo;
    for (FileIterator iter = (file).begin();
         iter != (file).end();
         ++iter) {
        Page page = *iter;
        pageNo = page.page_number();
        bufMgr->readPage(&file, pageNo, pagepoint);
        if(pagepoint->hasSpaceForRecord(tuple)){
            flag = true;
            record = pagepoint->insertRecord(tuple);
            bufMgr->unPinPage(&file, pageNo, true);
            break;
        }
        bufMgr->unPinPage(&file, pageNo, false);
    }
    //Create a new page
    if (!flag){
        bufMgr->allocPage(&file, pageNo, pagepoint);
        record = pagepoint->insertRecord(tuple);
        bufMgr->unPinPage(&file, pageNo, true);
    }
    return record;
}

void HeapFileManager::deleteTuple(const RecordId& rid,
                                  File& file,
                                  BufMgr* bufMgr) {
    cout<<"hahahha"<<endl;
    Page *page;
    bufMgr->readPage(&file, rid.page_number, page);
    page->deleteRecord(rid);
    bufMgr->unPinPage(&file, rid.page_number, true);
}

string HeapFileManager::createTupleFromSQLStatement(const string& sql,
                                                    const Catalog* catalog) {
    string pattern1 = " *'?([0-9A-Za-z]+)'?,";
	regex pattern("INSERT INTO ([0-9A-Za-z]) VALUES \\("+pattern1+"* *'?([0-9A-Za-z]+)'?\\);"); //\('([0-9A-Za-z]*)',([0-9A-Za-z]*)\)
	smatch results;
	unsigned int num = 0;
	string name = "";
	vector<string> var;
	vector<string> attr;//a list about tuples of attrs
	string tuple="";
    //regex for finding values
	if (regex_match(sql, results, pattern)) {
		for (auto it = results.begin(); it != results.end(); ++it){
			if(num == 1)
				name = *it; //table name
			else if(num > 1){
				var.push_back(*it); //table attr
			}
			num++;
		}
	}
	//add values
    const TableSchema table = catalog->getTableSchema(catalog->getTableId(name));
    char header = 0b00000000;//metadata
    tuple.insert(tuple.size(), 8, header);// final result
    int attr_num = table.getAttrCount();
    for(int i = 0; i < attr_num; i++){
        //get attribution
        int max_size = table.getAttrMaxSize(i);//size
        DataType data = table.getAttrType(i);//type {INT, CHAR, VARCHAR}
        string tuple_attr = var[i];// value of the ith variable
        if(data == 0){   //INT
            unsigned int a = 0;
            unsigned int b = 255;
            sscanf(tuple_attr.c_str(), "%d", &a);
            char c[4];
            for(int i = 0; i < 4; i++){
                c[3-i] = (a & b << 8*i) >> 8*i;
                tuple.insert(tuple.size() - i, 1, c[3-i]);
            }
            //c calculated

        }
        else if(data == 1){ //CHAR
            unsigned int minus = max_size - tuple_attr.length();
            unsigned int j = 0;
            for(; j < tuple_attr.length(); j++){
                tuple.insert(tuple.size(), 1, tuple_attr[j]);
            }
            for(unsigned int num = 0; num < minus; num++){
                tuple.insert(tuple.size(), 1, 0b00000000); //add zero
                j++;
            }
        }
        else if(data == 2){// VARCHAR
            tuple.insert(tuple.size(), to_string(tuple_attr.size()));
            unsigned int j = 0;
            for(; j < tuple_attr.length(); j++){
                tuple.insert(tuple.size(), 1, tuple_attr[j]);
            }
        }

        if(tuple.size() % 4 != 0){// extend to alias 4 byte
            tuple.insert(tuple.size(), 4 - tuple.size() % 4, 0b00000000);
        }
    }
    return tuple;
}
}  // namespace badgerdb
