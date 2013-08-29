
SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra")

#INCLUDE_DIRECTORIES()
ADD_LIBRARY(redis_vtbl SHARED redis_vtbl.c list.c vector.c address.c)
TARGET_LINK_LIBRARIES(redis_vtbl hiredis)

