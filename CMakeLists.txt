cmake_minimum_required(VERSION 3.8)
project(conjunctiveQ)


set(CMAKE_CXX_STANDARD 17)


set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -g -pthread -ltcmalloc -O3 ")


add_executable(ours
        main.cpp Table/Table.cpp Table/Table.h Table/Index.cpp Table/Index.h Exec/DB.cpp Exec/DB.h Query/Query.cpp Query/Query.h Table/TableContent.cpp Table/TableContent.h Util/util.cpp Util/util.h CDG/CDG.cpp CDG/CDG.h IO.h Util/argument_helper.cpp Util/argument_helper.h CDG/CDGEdge.cpp CDG/CDGNode.cpp CDG/CDGElement.h CDG/DecideOrder.cpp CDG/MonitorStat.cpp CDG/UpdateCDG.cpp )
set_target_properties(ours PROPERTIES
        COMPILE_FLAGS
        "-D our_method"
        )



target_include_directories(ours
        PUBLIC
        Table/
        CDG/
        Exec/
        Query/
        Util/
        tsl/
)


