
INCLUDE_DIRECTORIES(${PROJECT_SOURCE_DIR}/src)

SET(CMAKE_C_FLAGS "${CMAKE_C_FLAGS} -Wall -Wextra -g3 -O0")

MACRO(REGISTER_TEST testname)
	SET_TARGET_PROPERTIES(${testname} PROPERTIES RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
	ADD_TEST(${testname} "${MAKE_CURRENT_BINARY_DIR}/${testname}")
	ADD_DEPENDENCIES(check ${testname})
ENDMACRO()

ADD_EXECUTABLE(driver EXCLUDE_FROM_ALL driver.c)
TARGET_LINK_LIBRARIES(driver sqlite3)
REGISTER_TEST(driver)

ADD_EXECUTABLE(test_vector EXCLUDE_FROM_ALL test_vector.c ${PROJECT_SOURCE_DIR}/src/vector.c)
TARGET_LINK_LIBRARIES(test_vector m)
REGISTER_TEST(test_vector)

ADD_EXECUTABLE(test_list EXCLUDE_FROM_ALL test_list.c ${PROJECT_SOURCE_DIR}/src/list.c)
TARGET_LINK_LIBRARIES(test_list m)
REGISTER_TEST(test_list)

ADD_EXECUTABLE(test_address EXCLUDE_FROM_ALL test_address.c ${PROJECT_SOURCE_DIR}/src/address.c)
TARGET_LINK_LIBRARIES(test_address m)
REGISTER_TEST(test_address)

ADD_EXECUTABLE(test_connection EXCLUDE_FROM_ALL 
test_connection.c 
${PROJECT_SOURCE_DIR}/src/connection.c 
${PROJECT_SOURCE_DIR}/src/list.c 
${PROJECT_SOURCE_DIR}/src/vector.c 
${PROJECT_SOURCE_DIR}/src/address.c 
${PROJECT_SOURCE_DIR}/src/redis.c 
${PROJECT_SOURCE_DIR}/src/sentinel.c)
TARGET_LINK_LIBRARIES(test_connection m hiredis)
REGISTER_TEST(test_connection)

ADD_EXECUTABLE(test_format EXCLUDE_FROM_ALL test_format.c)
REGISTER_TEST(test_format)
