all: 
	g++ -std=c++11 -g StudentComponent/LogRecord.h
	g++ -std=c++11 -g StudentComponent/LogRecord.cpp -c -o LogRecord.o
	g++ -std=c++11 -g StudentComponent/LogMgr.h
	g++ -std=c++11 -g StudentComponent/LogMgr.cpp -c -o LogMgr.o
	g++ -std=c++11 -g StorageEngine/StorageEngine.h
	g++ -std=c++11 -g StorageEngine/StorageEngine.cpp -c -o StorageEngine.o
	g++ -std=c++11 -g StorageEngine/main.cpp StorageEngine.o LogMgr.o LogRecord.o -o main.o 


