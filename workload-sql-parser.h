#include "hashtable.h"

#ifndef WORKLOAD_TPCH
#define WORKLOAD_TPCH

#define NB_LINEITEMS 30

size_t get_db_size_sql_parser(void);

typedef struct node {
    char* val;
    struct node * next;
} field_node_t;

typedef enum type {
    SELECT
} type_t;

typedef enum operator {
    Eq,
    Ne,
    Gt,
    Lt,
    Gte,
    Lte,
} operator_t;

typedef struct condition
{
    char *operand1;
    bool operand1_is_field;
    operator_t operator;
    char *operand2;
    bool operand2_is_field;
    struct condition *next_condition;
} condition_t;

typedef struct query
{
    type_t type;
    char *table_name;
    field_node_t *field_ptr;
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

void print_query_object(query_t *query);

struct table_struct *test_table;

static char *add_column_int_value(char *item, char *column_name, int int_value);

char *add_column_string_value(char *item, char *column_name, char *string_value);

#endif
