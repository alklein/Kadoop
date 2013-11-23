.SUFFIXES: .java .class
.java.class:
	javac $<

CLASSES = WordCount_Mapper.class WordCount_Reducer.class DFS_test.class DFS_client.class UTILS/Chunk.class UTILS/Constants.class UTILS/Msg.class UTILS/Address.class UTILS/ChunkName.class DFS/NameNode.class DFS/DataNode.class DFS/AccessPoint.class MR/Mapper.class MR/Reducer.class MR/Master.class MR/ComputeNode.class

all: $(CLASSES)

WordCount_Mapper: WordCount_Mapper.class
WordCount_Reducer: WordCount_Reducer.class
ComputeNode: ComputeNode.class

Test: test.class
Client: client.class

Chunk: Chunk.class
Constants: Constants.class
Msg: Msg.class
Address: Address.class
ChunkName: ChunkName.class

NameNode: NameNode.class
DataNode: DataNode.class
AccessPoint: AccessPoint.class

Mapper: Mapper.class
Reducer: Reducer.class
Master: Master.class

clean:
	rm $(CLASSES)

