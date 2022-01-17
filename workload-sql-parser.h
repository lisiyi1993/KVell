#include "hashtable.h"
#include "hashset.h"
#include "hashset_itr.h"
#include <regex.h>

#ifndef WORKLOAD_TPCH
#define WORKLOAD_TPCH

#define NB_LINEITEMS 10

size_t get_db_size_sql_parser(void);

ht *sql_tables_columns;

typedef struct node 
{
   char *key;
   char *val;
   struct node *next;
} list_node_t;

typedef enum type 
{
   SELECT
} type_t;

typedef enum operator 
{
   EQ,
   NE,
   GT,
   LT,
   GTE,
   LTE,
   LIKE,
   IN,
   BETWEEN,
} operator_t;

typedef enum value_type 
{
   CONSTANT,
   ARITHMETIC,
   COLUMN_FIELD
} value_t;

typedef struct arithmetic_condition
{
   char *operand1;
   char *operand2;
   char *operator;
} arithmetic_condition_t;

typedef struct comparison_condition
{
   void *value;
   value_t value_type;
   char *table;
} comparison_condition_t;

typedef struct like_condition
{
   char *ex;
   regex_t regex;
} like_condition_t;

typedef struct in_condition
{
   list_node_t *match_ptr;
} in_condition_t;

typedef struct between_condition
{
   void *min_value;
   value_t min_value_type;
   void *max_value;
   value_t max_value_type;
} between_condition_t;

typedef struct field_operand
{
   char *name;
   char *table;
} field_operand_t;

typedef struct condition
{
   field_operand_t *operand1;
   operator_t operator;
   void *operand2;
   struct condition *next_condition;
   bool not;
} condition_t;

typedef struct table_name
{
   char *name;
   char alt_name[256];
   struct table_name *next_table;
} table_name_t;

typedef struct query
{
   type_t type;
   table_name_t *table_name_ptr;
   list_node_t *field_ptr;
   condition_t *condition_ptr;
   condition_t *and_condition_ptr;
   condition_t *or_condition_ptr;
} query_t;

struct table_struct {
   char *name;
   ht* column_map;
};

typedef enum {
   INT, 
   STRING
} data_type;

struct column_info
{
   int index;
   data_type type;
};


query_t* parse_sql(char *ptr);

query_t* query;

struct table_struct *test_table;

void print_query_object(query_t *query);

void create_test_table(long num_columns, char input_columns[][100], char input_columns_type[][100]);

void create_sql_tables_columns();

#endif
