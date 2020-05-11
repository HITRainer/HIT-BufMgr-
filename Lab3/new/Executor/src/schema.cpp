/**
 * @author Zhaonian Zou <znzou@hit.edu.cn>,
 * School of Computer Science and Technology,
 * Harbin Institute of Technology, China
 */

#include "schema.h"

#include <string>
#include <sstream>

using namespace std;

namespace badgerdb {

TableSchema TableSchema::fromSQLStatement(const string& sql) {
    string tableName;
    vector<Attribute> attrs;
    bool isTemp = false;
  //获得Table名字
    unsigned int index1 = sql.find("CREATE TABLE");
	unsigned int index2 = sql.find("(", 0, 1);
	unsigned int index3 = sql.length();
	unsigned int j = index2 + 1; //用来分类变量的
	unsigned int maxSize = 0;
	bool flag = false;
	bool isnotNull = false;
	bool isUnique = false;
	DataType data;
	string var = "";
	string temp = "";
	stringstream ss;

	for(unsigned int i = index1 + 12; i < index2; i++){
		if(sql[i] != ' ')
			tableName += sql[i];//获得名字 name
	}
	while(j < sql.length()){
		index3 = sql.find(",", index2 + 1, 1); //找到第一个 ','
		if (index3 > sql.length())
			index3 = sql.find(";", index2 + 1, 1);
		temp = sql.substr(index2 + 1, index3 - index2 - 1);
		if(sql[j] == ' ' && flag){//说明参数名字找完了
			if(temp.find("UNIQUE", 0) != string::npos){
				isUnique = true;
			}
			if(temp.find("NOT NULL", 0) != string::npos){
				isnotNull = true;
			}
			/*数据类型*/
			if(temp.find("INT", 0) != string::npos){
				maxSize = 4;
				data = INT;
			}
			else if(temp.find("VARCHAR", 0) != string::npos){
				unsigned int start = temp.find("VARCHAR(", 0) + 8;
				unsigned int end = temp.find(")", 0, 1);
				string length = temp.substr(start, end - start);
				//sscanf(length, "%d", &maxSize);
				ss<<length;
				ss>>maxSize;
				data = VARCHAR;
			}
			else if(temp.find("CHAR", 0) != string::npos){
				unsigned int start = temp.find("CHAR(", 0) + 5;
				unsigned int end = temp.find(")", 0, 1);
				string length = temp.substr(start, end - start);
				//sscanf(length, "%d", &maxSize);
				ss<<length;
				ss>>maxSize;
				data = CHAR;
			}
            //Construct Attr
            Attribute* attr = new Attribute(var, data, maxSize, isnotNull, isUnique);
            attrs.push_back(*attr);
			var = "";
			flag = false;
			index2 = index3;
			j = index2 + 1;
			isnotNull = false;
			isUnique = false;

		}
		else if(sql[j] != ' '){
			var += sql[j];
			flag = true;
		}
		j++;
	}
  return TableSchema(tableName, attrs, isTemp);
}

void TableSchema::print() const {
  printf("tableName: ");
  printf("%s\n", (tableName.c_str()));
  for(unsigned int i = 0 ; i < attrs.size(); i++){
    printf("attr %d: %s",i ,(attrs[i].attrName.c_str()));
  }
  printf("\n");
}

}  // namespace badgerdb
