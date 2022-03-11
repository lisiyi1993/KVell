#include "hashtable.h"
#include "hashset.h"
#include "hashset_itr.h"
#include <regex.h>

#ifndef WORKLOAD_TPCH
#define WORKLOAD_TPCH

#define DEFAULT_INT_VALUE -1
#define DEFAULT_STRING_VALUE ""

#define NB_LINEITEMS 100
#define NB_ORDERS 10
#define NB_CUSTOMER 4

size_t get_db_size_sql_parser(void);

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
   SUM,
   AVG,
   COLUMN_FIELD
} value_t;

typedef struct node 
{
   char *key;
   char *val;
   char *as;
   value_t value_type;
   struct node *next;
} list_node_t;

typedef struct group_by_node 
{
   char *key;
   char *val;
   struct order_by_node *next;
} group_by_node_t;

typedef struct order_by_node 
{
   char *key;
   char *val;
   bool desc;
   struct order_by_node *next;
} order_by_node_t;

typedef struct result_node
{
   char *item;
   struct result_node *next;
} sql_result_node_t;

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
   char *table_identifier;
} field_operand_t;

typedef struct condition
{
   field_operand_t *operand1;
   operator_t operator;
   void *operand2;
   struct condition *next_condition;
   bool not;
   bool is_join_condition;
} condition_t;

typedef struct table_name
{
   char *name;
   char identifier[256];
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
   group_by_node_t *group_by_ptr;
   order_by_node_t *order_by_ptr;
} query_t;

typedef struct table_struct {
   char *name;
   ht* column_map;
   int start_index;
   int end_index;
} table_t;

typedef enum {
   INT, 
   STRING
} data_type;

struct column_info
{
   int index;
   data_type type;
};

typedef struct group_by
{
   char **item_array;
   int last_index;
} group_by_t;

ht *sql_tables_columns;
ht *table_identifier_to_table_name;

query_t* origin_query;
query_t* query;

query_t *sub_query_list[2];
ht *sub_queries_map;

table_t *lineitem_table;

table_t *orders_table;

table_t *customer_table;

sql_result_node_t *lineitem_result_list;
sql_result_node_t *cur_lineitem_result_item;

sql_result_node_t *orders_result_list;
sql_result_node_t *cur_orders_result_item;

sql_result_node_t *customer_result_list;
sql_result_node_t *cur_customer_result_item;

void parse_sql(char *ptr);

void print_query_object(query_t *query);

void create_lineitem_table(long num_columns, char input_columns[][100], char input_columns_type[][100]);

void create_orders_table(long num_columns, char input_columns[][100], char input_columns_type[][100]);

void create_sql_tables_columns();

void create_table_identifier_to_table_name();

#endif
