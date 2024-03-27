set(module cdb2import)
set(MODULE CDB2IMPORT)
configure_file(${PROJECT_SOURCE_DIR}/mem/mem.h.in mem_util.h @ONLY)
set(src
	import.c
)
add_library(cdb2import ${src})
include_directories(
  ${PROJECT_SOURCE_DIR}/bbinc
  ${PROJECT_SOURCE_DIR}/crc32c
)
add_dependencies(cdb2import mem crc32c)
