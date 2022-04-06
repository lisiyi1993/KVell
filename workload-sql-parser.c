/*
 * SQL PARSER workload
 */

#include "headers.h"
#include "workload-common.h"
#include "workload-sql-parser.h"
#include "hashtable.h"
#include "hashset.h"
#include "hashset_itr.h"
#include <string.h>

/*
 * File contains:
 * 1/ Definitions of tables and columns
 * 2/ Functions to generate keys for items stored in the KV (e.g., a unique key for an item or a warehouse)
 * 3/ Loader functions
 * 4/ Queries
 */

/*
 * SQL PARSER has a few tables and columns:
 */
enum table { LINEITEM, PART, PARTSUPP, ORDERS, SUPPLIERS, CUSTOMER, NATION, REGION, TMP };

enum column {
   ORDERKEY, PARTKEY, SUPPKEY, LINENUMBER, QUANTITY, EXTENDEDPRICE, DISCOUNT, TAX, RETURNFLAG, LINESTATUS, SHIPDATE, COMMITDATE, RECEIPTDATE, SHIPINSTRUCT, SHIPMODE, COMMENT,
};

void create_sql_tables_columns() 
{
   sql_tables_columns = ht_create();
}

void create_table_identifier_to_table_name()
{
   table_identifier_to_table_name = ht_create();
}

void create_lineitem_table(long num_columns, char input_columns[][100], char input_columns_type[][100]) 
{
   lineitem_table = (table_t *) calloc(1, sizeof(table_t));
   lineitem_table->name = "lineitem";
   lineitem_table->start_index = 0;
   lineitem_table->end_index = NB_LINEITEMS;

   lineitem_table->column_map = ht_create();
   for (size_t i = 0; i < num_columns; i++)
   {
      // int *index = malloc(sizeof(int));
      // *index = i;

      struct column_info *ci = calloc(1, sizeof(ci));
      ci->index = i;

      if (strcmp(input_columns_type[i], "INT") == 0) 
      {
         ci->type = INT;
      }
      else if (strcmp(input_columns_type[i], "STRING") == 0)
      {
         ci->type = STRING;
      }
      
      // if (test_table->column_map == NULL) {
      //    printf("yes it is null\n");
      // }
      ht_set(lineitem_table->column_map, input_columns[i], ci);
      // struct column_info *ci_get = ht_get(test_table->column_map, input_columns[i]);
      // printf("here \n");
      // printf("%s and (%d)\n", input_columns[i], ci_get->index);
      // printf("done\n");
   }
}

void create_orders_table(long num_columns, char input_columns[][100], char input_columns_type[][100]) 
{
   orders_table = (table_t *) calloc(1, sizeof(table_t));
   orders_table->name = "orders";
   orders_table->start_index = NB_LINEITEMS - 1;
   orders_table->end_index = NB_LINEITEMS + NB_ORDERS;

   orders_table->column_map = ht_create();
   for (size_t i = 0; i < num_columns; i++)
   {
      struct column_info *ci = calloc(1, sizeof(ci));
      ci->index = i;

      if (strcmp(input_columns_type[i], "INT") == 0) 
      {
         ci->type = INT;
      }
      else if (strcmp(input_columns_type[i], "STRING") == 0)
      {
         ci->type = STRING;
      }

      ht_set(orders_table->column_map, input_columns[i], ci);
   }
}

void create_cutomer_table(long num_columns, char input_columns[][100], char input_columns_type[][100]) 
{
   customer_table = (table_t *) calloc(1, sizeof(table_t));
   customer_table->name = "customer";
   customer_table->start_index = NB_LINEITEMS + NB_ORDERS - 1;
   customer_table->end_index = NB_LINEITEMS + NB_ORDERS + NB_CUSTOMER;

   customer_table->column_map = ht_create();
   for (size_t i = 0; i < num_columns; i++)
   {
      struct column_info *ci = calloc(1, sizeof(ci));
      ci->index = i;

      if (strcmp(input_columns_type[i], "INT") == 0) 
      {
         ci->type = INT;
      }
      else if (strcmp(input_columns_type[i], "STRING") == 0)
      {
         ci->type = STRING;
      }

      ht_set(customer_table->column_map, input_columns[i], ci);
   }
}

void create_nation_table(long num_columns, char input_columns[][100], char input_columns_type[][100]) 
{
   nation_table = (table_t *) calloc(1, sizeof(table_t));
   nation_table->name = "nation";
   nation_table->start_index = NB_LINEITEMS + NB_ORDERS + NB_CUSTOMER - 1;
   nation_table->end_index = NB_LINEITEMS + NB_ORDERS + NB_CUSTOMER + NB_NATION;

   nation_table->column_map = ht_create();
   for (size_t i = 0; i < num_columns; i++)
   {
      struct column_info *ci = calloc(1, sizeof(ci));
      ci->index = i;

      if (strcmp(input_columns_type[i], "INT") == 0) 
      {
         ci->type = INT;
      }
      else if (strcmp(input_columns_type[i], "STRING") == 0)
      {
         ci->type = STRING;
      }

      ht_set(nation_table->column_map, input_columns[i], ci);
   }
}

void create_region_table(long num_columns, char input_columns[][100], char input_columns_type[][100]) 
{  
   region_table = (table_t *) calloc(1, sizeof(table_t));
   region_table->name = "region";
   region_table->start_index = NB_LINEITEMS + NB_ORDERS + NB_CUSTOMER + NB_NATION - 1;
   region_table->end_index = NB_LINEITEMS + NB_ORDERS + NB_CUSTOMER + NB_NATION + NB_REGION;

   region_table->column_map = ht_create();
   for (size_t i = 0; i < num_columns; i++)
   {
      struct column_info *ci = calloc(1, sizeof(ci));
      ci->index = i;

      if (strcmp(input_columns_type[i], "INT") == 0) 
      {
         ci->type = INT;
      }
      else if (strcmp(input_columns_type[i], "STRING") == 0)
      {
         ci->type = STRING;
      }

      ht_set(region_table->column_map, input_columns[i], ci);
   }
}

char *add_column_value(char *item, char *column_name, void *value, table_t *table) {
   struct column_info *ci = ht_get(table->column_map, column_name);
   switch (ci->type)
   {
   case INT:
      return add_shash_uint(item, *((int *) ht_get(table->column_map, column_name)), value, 1);
      break;
   case STRING:
      return add_shash_specific_string(item, *((int *) ht_get(table->column_map, column_name)), value, 1);
      break;
   default:
      break;
   }
}

static char* get_column_string_value(char* item, char* column_name, table_t *table) {
   // printf("get_column_string_value\n"); 
   // dump_shash(item);

   struct column_info *ci = ht_get(table->column_map, column_name);
   char *tmp = (char *) calloc(1, sizeof(char));

   if (ci == NULL)
   {
      printf("ci is null, column_name = %s\n", column_name);
   }

   switch (ci->type)
   {
   case INT:
      // printf("get_column_string_value INT %d\n", ci->index);
      sprintf(tmp, "%ld", get_shash_uint(item, ci->index));
      break;
   case STRING:
      // printf("get_column_string_value STRING %d, %s\n", ci->index, get_shash_string(item, ci->index));
      sprintf(tmp, "%s", get_shash_string(item, ci->index));
      break;
   default:
      break;
   }

   return tmp;
}

bool compare_column_value(char *item, char *column_name, operator_t operator, void *value_to_compare, bool not, table_t *table) {
   struct column_info *ci = ht_get(table->column_map, column_name);
   void *item_value = get_column_string_value(item, column_name, table);

   bool res;
   switch (operator)
   {
      case EQ: 
      {
         switch (ci->type)
         {
            case INT:
               res = get_shash_uint(item, ci->index) == atoi(value_to_compare);
               break;
            case STRING:
               res = strcmp(get_shash_string(item, ci->index), (char *) value_to_compare) == 0;
               break;
            default:
               break;
         }
         break;
      }
      case GT: 
      {
         switch (ci->type)
         {
            case INT:
               res = get_shash_uint(item, ci->index) > atoi(value_to_compare);
               break;
            case STRING:
               res = strcmp(get_shash_string(item, ci->index), (char *) value_to_compare) > 0;
               break;
            default:
               break;
         }
         break;
      }
      case LT: 
      {
         switch (ci->type)
         {
            case INT:
               res = get_shash_uint(item, ci->index) < atoi(value_to_compare);
               break;
            case STRING:
               res = strcmp(get_shash_string(item, ci->index), (char *) value_to_compare) < 0;
               break;
            default:
               break;
         }
         break;
      }
      case GTE: 
      {
         switch (ci->type)
         {
            case INT:
               res = get_shash_uint(item, ci->index) >= atoi(value_to_compare);
               break;
            case STRING:
               res = strcmp(get_shash_string(item, ci->index), (char *) value_to_compare) >= 0;
               break;
            default:
               break;
         }
         break;
      }
      case LTE: 
      {
         switch (ci->type)
         {
            case INT:
               res = get_shash_uint(item, ci->index) <= atoi(value_to_compare);
               break;
            case STRING:
               res = strcmp(get_shash_string(item, ci->index), (char *) value_to_compare) <= 0;
               break;
            default:
               break;
         }
         break;
      }
      case NE:
      {
         switch (ci->type)
         {
            case INT:
               res = get_shash_uint(item, ci->index) != atoi(value_to_compare);
               break;
            case STRING:
               res = strcmp(get_shash_string(item, ci->index), (char *) value_to_compare) != 0;
               break;
            default:
               break;
         }
         break;
      }
      case LIKE:
      {
         int reti = regexec(&(((like_condition_t *) value_to_compare)->regex), get_shash_string(item, ci->index), 0, NULL, 0);
         if (reti == 0) {
            res = true;
         }
         else {
            res = false;
         }
         break;
      }
      case BETWEEN:
      {
         between_condition_t *between_condition = (between_condition_t *) value_to_compare;
         long item_value = get_shash_uint(item, ci->index);
         switch (ci->type)
         {
            case INT:
               res = atol(between_condition->min_value) <= item_value && item_value <= atol(between_condition->max_value);
               break;
            default:
               break;
         }
         break;
      }
      case IN: 
      {
         list_node_t *cur_node = ((in_condition_t *) value_to_compare)->match_ptr;
         res = false;
         while (cur_node != NULL) 
         {
            switch (ci->type)
            {
               case INT:
                  res = get_shash_uint(item, ci->index) == atoi(cur_node->val);
                  break;
               case STRING:
                  res = strcmp(get_shash_string(item, ci->index), cur_node->val) == 0;
                  break;
               default:
                  break;
            }
            if (res) {
               break;
            }
            cur_node = cur_node->next;
         }
         break;
      }
      default: {
         res = false;
         break;
      }
   }
   
   if (not) {
      return !res;
   }
   else {
      return res;
   }
}

char* perform_arithmetic(arithmetic_condition_t *arithmetic_condition) {
   char *result = "";
   if (strcmp(arithmetic_condition->operator, "+") == 0) 
   {
      asprintf(&result, "%d", atoi(arithmetic_condition->operand1) + atoi(arithmetic_condition->operand2));
   }
   else if (strcmp(arithmetic_condition->operator, "-") == 0) 
   {
      asprintf(&result, "%d", atoi(arithmetic_condition->operand1) - atoi(arithmetic_condition->operand2));
   }
   else if (strcmp(arithmetic_condition->operator, "*") == 0) 
   {
      asprintf(&result, "%d", atoi(arithmetic_condition->operand1) * atoi(arithmetic_condition->operand2));
   }
   else if (strcmp(arithmetic_condition->operator, "/") == 0) 
   {
      asprintf(&result, "%d", atoi(arithmetic_condition->operand1) / atoi(arithmetic_condition->operand2));
   }

   return result;
}

bool evaluate_where_condition(char *item, char *column_name, condition_t *condition, table_t *table) {
   struct column_info *ci = ht_get(table->column_map, column_name);
   switch (condition->operator)
   {
      case EQ:
      case GT:
      case LT:
      case GTE:
      case LTE:
      case NE:
      {
         comparison_condition_t *comparison_condition = (comparison_condition_t *) condition->operand2;
         switch (comparison_condition->value_type)
         {
            case CONSTANT:
            {
               return compare_column_value(item, column_name, condition->operator, (char *) comparison_condition->value, condition->not, table);
               break;
            }
            case ARITHMETIC:
            {
               arithmetic_condition_t *arithmetic_condition = (arithmetic_condition_t *) comparison_condition->value;
               char *expected_value = perform_arithmetic(arithmetic_condition);
               return compare_column_value(item, column_name, condition->operator, expected_value, condition->not, table);
               break;
            }
            default:
            {
               break;
            }
         }
         break;
      }
      case BETWEEN:
      {
         between_condition_t *old_between_condition = (between_condition_t *) condition->operand2;
         between_condition_t *new_between_condition = (between_condition_t *) calloc(1, sizeof(between_condition_t));
         new_between_condition->min_value_type = CONSTANT;
         new_between_condition->max_value_type = CONSTANT;

         switch (old_between_condition->min_value_type)
         {
            case CONSTANT:
            {
               new_between_condition->min_value = old_between_condition->min_value;
               break;
            }
            case ARITHMETIC:
            {
               arithmetic_condition_t *arithmetic_condition = (arithmetic_condition_t *)(old_between_condition->min_value);
               new_between_condition->min_value = perform_arithmetic(arithmetic_condition);
               break;
            }
            default:
               break;
         }

         switch (old_between_condition->max_value_type)
         {
            case CONSTANT:
            {
               new_between_condition->max_value = old_between_condition->max_value;
               break;
            }
            case ARITHMETIC:
            {
               arithmetic_condition_t *arithmetic_condition = (arithmetic_condition_t *)(old_between_condition->max_value);
               new_between_condition->max_value = perform_arithmetic(arithmetic_condition);
               break;
            }
            default:
               break;
         }

         return compare_column_value(item, column_name, condition->operator, new_between_condition, condition->not, table);
      }
      default:
      {
         return compare_column_value(item, column_name, condition->operator, condition->operand2, condition->not, table);
         break;
      }
   }
}

/*
 * SQL PARSER loader
 * Generate all the elements that will be stored in the DB
 */

/*
 * We want every DB insert to have a unique and identifiable key. These functions generate a unique key depending on the table, column and primary key(s) of a tuple.
 */
struct sql_parser_key {
   union {
      long key;
      struct {
         unsigned long prim_key:60;
         unsigned int table:4;
      };
   };
} __attribute__((packed));

long sql_parser_get_key_lineitem(long linenumber) {
   struct sql_parser_key k = {
      .table = LINEITEM,
      .prim_key = linenumber
   };
   return k.key;
}

long sql_parser_get_key_order(long order) {
   struct sql_parser_key k = {
      .table = ORDERS,
      .prim_key = order
   };
   return k.key;
}

long sql_parser_get_key_customer(long customer) {
   struct sql_parser_key k = {
      .table = CUSTOMER,
      .prim_key = customer
   };
   return k.key;
}

long sql_parser_get_key_nation(long nation) {
   struct sql_parser_key k = {
      .table = NATION,
      .prim_key = nation
   };
   return k.key;
}

long sql_parser_get_key_region(long region) {
   struct sql_parser_key k = {
      .table = REGION,
      .prim_key = region
   };
   return k.key;
}

long sql_parser_get_key_tmp(long tmp) {
   struct sql_parser_key k = {
      .table = TMP,
      .prim_key = tmp
   };
   return k.key;
}

static char *sql_parser_lineitem(uint64_t uid) {
   uint64_t key = sql_parser_get_key_lineitem(uid);
   char *item = create_shash(key);
   return item;
}

static char *sql_parser_order(uint64_t uid) {
   uint64_t key = sql_parser_get_key_order(uid);
   char *item = create_shash(key);
   return item;
}

static char *sql_parser_customer(uint64_t uid) {
   uint64_t key = sql_parser_get_key_customer(uid);
   char *item = create_shash(key);
   return item;
}

static char *sql_parser_nation(uint64_t uid) {
   uint64_t key = sql_parser_get_key_nation(uid);
   char *item = create_shash(key);
   return item;
}

static char *sql_parser_region(uint64_t uid) {
   uint64_t key = sql_parser_get_key_region(uid);
   char *item = create_shash(key);
   return item;
}

static char *sql_parser_tmp(uint64_t uid) {
   uint64_t key = sql_parser_get_key_tmp(uid);
   char *item = create_shash(key);
   return item;
}

char *sql_parser_add_lineitem(uint64_t uid) {
   char *item = sql_parser_lineitem(uid);

   item = add_column_value(item, "TABLE", "lineitem", lineitem_table);
   item = add_column_value(item, "ORDERKEY", rand_between(0, NB_ORDERS), lineitem_table);
   item = add_column_value(item, "LINENUMBER", uid & NB_LINEITEMS, lineitem_table);
   item = add_column_value(item, "QUANTITY", rand_between(1, 100), lineitem_table);
   item = add_column_value(item, "DISCOUNT", rand_between(100, 999), lineitem_table);
   item = add_column_value(item, "TAX", rand_between(1, 30), lineitem_table);

   if (uid % 3 == 0) {
      item = add_column_value(item, "SHIPDATE", "2021-09-03", lineitem_table);
      item = add_column_value(item, "LINESTATUS", "1", lineitem_table);
   }
   else if (uid % 3 == 1) {
      item = add_column_value(item, "SHIPDATE", "2021-09-04", lineitem_table);
      item = add_column_value(item, "LINESTATUS", "2", lineitem_table);
   }
   else {
      item = add_column_value(item, "SHIPDATE", "2021-11-05", lineitem_table);
      item = add_column_value(item, "LINESTATUS", "3", lineitem_table);
   }

   if (uid % 2 == 0)
   {
      item = add_column_value(item, "RETURNFLAG", "A", lineitem_table);
   }
   else 
   {
      item = add_column_value(item, "RETURNFLAG", "R", lineitem_table);
   }
   
   item = add_column_value(item, "PARTKEY", DEFAULT_INT_VALUE, lineitem_table);
   item = add_column_value(item, "SUPPKEY", DEFAULT_INT_VALUE, lineitem_table);
   item = add_column_value(item, "EXTENDEDPRICE", rand_between(1, 2000), lineitem_table);
   item = add_column_value(item, "COMMITDATE", DEFAULT_STRING_VALUE, lineitem_table);
   item = add_column_value(item, "RECEIPTDATE", DEFAULT_STRING_VALUE, lineitem_table);
   item = add_column_value(item, "SHIPINSTRUCT", DEFAULT_STRING_VALUE, lineitem_table);
   item = add_column_value(item, "SHIPMODE", DEFAULT_STRING_VALUE, lineitem_table);
   item = add_column_value(item, "COMMENT", DEFAULT_STRING_VALUE, lineitem_table);

   return item;
}

char *sql_parser_add_order(uint64_t uid, char *order_date) {
   char *item = sql_parser_order(uid);
   item = add_column_value(item, "TABLE", "orders", orders_table);
   item = add_column_value(item, "ORDERKEY", uid % NB_ORDERS, orders_table);
   item = add_column_value(item, "CUSTKEY", rand_between(0, NB_CUSTOMER), orders_table);
   item = add_column_value(item, "ORDERSTATUS", DEFAULT_STRING_VALUE, orders_table);
   item = add_column_value(item, "TOTALPRICE", DEFAULT_INT_VALUE, orders_table);
   item = add_column_value(item, "ORDERPRIORITY", DEFAULT_STRING_VALUE, orders_table);
   item = add_column_value(item, "CLERK", DEFAULT_STRING_VALUE, orders_table);
   item = add_column_value(item, "SHIPPRIORITY", rand_between(1, 5), orders_table);
   item = add_column_value(item, "ORDERDATE", order_date, orders_table);

   return item;
}

char *sql_parser_add_customer(uint64_t uid, char *name, char *address, char *phone, char *nation_key) 
{
   char *item = sql_parser_customer(uid);

   item = add_column_value(item, "CUSTKEY", uid % NB_CUSTOMER, customer_table);
   item = add_column_value(item, "TABLE", "customer", customer_table);
   item = add_column_value(item, "ACCTBAL", DEFAULT_STRING_VALUE, customer_table);
   item = add_column_value(item, "MKTSEGMENT", DEFAULT_STRING_VALUE, customer_table);
   item = add_column_value(item, "NAME", name, customer_table);
   item = add_column_value(item, "ADDRESS", address, customer_table);
   item = add_column_value(item, "PHONE", phone, customer_table);
   item = add_column_value(item, "NATIONKEY", nation_key, customer_table);

   return item;
}

char *sql_parser_add_nation(uint64_t uid, char *nation_key, char *nation_name, char *region_key)
{
   char *item = sql_parser_nation(uid);
   item = add_column_value(item, "TABLE", "nation", nation_table);
   item = add_column_value(item, "NATIONKEY", nation_key, nation_table);
   item = add_column_value(item, "NATIONNAME", nation_name, nation_table);
   item = add_column_value(item, "REGIONKEY", region_key, nation_table);

   return item;
}

char *sql_parser_add_region(uint64_t uid, char *region_key, char *region_name)
{
   char *item = sql_parser_region(uid);
   item = add_column_value(item, "TABLE", "region", region_table);
   item = add_column_value(item, "REGIONKEY", region_key, region_table);
   item = add_column_value(item, "REGIONNAME", region_name, region_table);

   return item;
}


/*
 * Function called by the workload-common interface. uid goes from 0 to the number of elements in the DB.
 */
static char* create_unique_item_sql_parser(uint64_t uid, uint64_t max_uid) {
   // printf("create_unique_item_sql_parser\n");
   // printf("uid=%d, max_uid=%d\n", uid, max_uid);
   if(uid == 0)
      assert(max_uid == get_db_size_sql_parser()); // set .nb_items_in_db = get_db_size_sql_parser() in main.c otherwise the DB will be partially populated

   if (uid < NB_LINEITEMS) 
   {
      return sql_parser_add_lineitem(uid);
   }
   else if (NB_LINEITEMS <= uid && uid < NB_LINEITEMS + NB_ORDERS)
   {
      if (uid % 8 == 0) 
      {
         return sql_parser_add_order(uid, "2021-01-01");
      }
      else if (uid % 8 == 1) 
      {
         return sql_parser_add_order(uid, "2021-02-02");
      }
      else if (uid % 8 == 2) 
      {
         return sql_parser_add_order(uid, "2021-03-03");
      }
      else if (uid % 8 == 3) 
      {
         return sql_parser_add_order(uid, "2021-04-04");
      }
      else if (uid % 8 == 4) 
      {
         return sql_parser_add_order(uid, "2021-05-05");
      }
      else if (uid % 8 == 5) 
      {
         return sql_parser_add_order(uid, "2021-06-16");
      }
      else if (uid % 8 == 6) 
      {
         return sql_parser_add_order(uid, "2021-07-17");
      }
      else if (uid % 8 == 7) 
      {
         return sql_parser_add_order(uid, "2021-08-08");
      }
   }
   else if (NB_LINEITEMS + NB_ORDERS <= uid && uid < NB_LINEITEMS + NB_ORDERS + NB_CUSTOMER)
   {  
      if (uid % NB_CUSTOMER == 0)
      {         
         return sql_parser_add_customer(uid, "Rene Howe", "3242 Goldcliff Circle", "(418)681-5378", "CA");
      }
      else if (uid % NB_CUSTOMER == 1)
      {
         return sql_parser_add_customer(uid, "Doreen Burns", "642 Arbor Court", "(514)552-6526", "CA");
      }
      else if (uid % NB_CUSTOMER == 2)
      {
         return sql_parser_add_customer(uid, "Billy Park", "1866 Wright Court", "(613)524-2588", "CA");
      }
      else if (uid % NB_CUSTOMER == 3)
      {
         return sql_parser_add_customer(uid, "Ray Audet", "1085 Par Drive", "(226)203-7946", "CA");
      }
      else if (uid % NB_CUSTOMER == 4)
      {
         return sql_parser_add_customer(uid, "Darcy Lamarre", "2910 State Street", "(819)728-7047", "US");
      }
      else if (uid % NB_CUSTOMER == 5)
      {
         return sql_parser_add_customer(uid, "Victoria Li", "1478 Peck Street", "(705)571-3996",  "US");
      }
      else if (uid % NB_CUSTOMER == 6)
      {
         return sql_parser_add_customer(uid, "Doreen Wiens",  "3523 August Lane", "(905)366-4553", "US");
      }
      else if (uid % NB_CUSTOMER == 7)
      {
         return sql_parser_add_customer(uid, "Anton McNeil", "1723 Murphy Court", "(204)382-2614", "US");
      }
      else if (uid % NB_CUSTOMER == 8)
      {
         return sql_parser_add_customer(uid, "Faye Hutchinson", "3018 Duke Lane", "(587)324-5441", "FR");
      }
      else if (uid % NB_CUSTOMER == 9)
      {
         return sql_parser_add_customer(uid, "Graham Kumar", "3637 Liberty Street", "(418)387-5161", "FR");
      }
      else if (uid % NB_CUSTOMER == 10)
      {
         return sql_parser_add_customer(uid, "Rob Meunier", "2707 Webster Street", "(780)585-6680", "FR");
      }
      else if (uid % NB_CUSTOMER == 11)
      {
         return sql_parser_add_customer(uid, "Corey Barr", "3762 Emma Street", "(819)383-2600", "FR");
      }
      else if (uid % NB_CUSTOMER == 12)
      {
         return sql_parser_add_customer(uid, "Sean Fowler", "1521 Colonial Drive", "(905)315-3888", "UK");
      }
      else if (uid % NB_CUSTOMER == 13)
      {
         return sql_parser_add_customer(uid, "Morgan Simon", "64 Straford Park", "(705)594-5525", "UK");
      }
      else if (uid % NB_CUSTOMER == 14)
      {
         return sql_parser_add_customer(uid, "Olive Charbonneau", "593 Carriage Court", "(902)265-2507", "UK");
      }
      else if (uid % NB_CUSTOMER == 15)
      {
         return sql_parser_add_customer(uid, "Graham Major", "2418 McVaney Road", "(306)279-6029", "UK");
      }
      else if (uid % NB_CUSTOMER == 16)
      {
         return sql_parser_add_customer(uid, "Bonnie Lachance", "3304 Maple Court", "(418)348-9187", "CN");
      }
      else if (uid % NB_CUSTOMER == 17)
      {
         return sql_parser_add_customer(uid, "Margaret Wu", "3353 Church Street", "(867)580-5248", "CN");
      }
      else if (uid % NB_CUSTOMER == 18)
      {
         return sql_parser_add_customer(uid, "James Baxter", "1893 Jennifer Lane", "(604)382-4350", "CN");
      }
      else if (uid % NB_CUSTOMER == 19)
      {
         return sql_parser_add_customer(uid, "Sarah Doucet", "2514 Rinehart Road", "(416)596-1275", "CN");
      }
      else if (uid % NB_CUSTOMER == 20)
      {
         return sql_parser_add_customer(uid, "Janice McGregor", "4887 Red Bud Lane", "(780)535-6149", "JP");
      }
      else if (uid % NB_CUSTOMER == 21)
      {
         return sql_parser_add_customer(uid, "Herb Ford", "3978 Quilly Lane", "(905)847-1235", "JP");
      }
      else if (uid % NB_CUSTOMER == 22)
      {
         return sql_parser_add_customer(uid, "Cory McMillan", "3675 Cedar Lane", "(902)569-8853", "JP");
      }
      else if (uid % NB_CUSTOMER == 23)
      {
         return sql_parser_add_customer(uid, "Jared Patry", "2764 Harter Street", "(226)494-7698", "JP");
      }
   }
   else if (NB_LINEITEMS + NB_ORDERS + NB_CUSTOMER <= uid && uid < NB_LINEITEMS + NB_ORDERS + NB_CUSTOMER + NB_NATION) 
   {
      if (uid % NB_NATION == 0)
      {
         return sql_parser_add_nation(uid, "CA", "Canada", "NA");
      }
      else if (uid % NB_NATION == 1)
      {
         return sql_parser_add_nation(uid, "US", "United States", "NA"); 
      }
      else if (uid % NB_NATION == 2)
      {
         return sql_parser_add_nation(uid, "FR", "France", "EU");
      }
      else if (uid % NB_NATION == 3)
      {
         return sql_parser_add_nation(uid, "UK", "United Kingdom", "EU");
      }
      else if (uid % NB_NATION == 4)
      {
         return sql_parser_add_nation(uid, "CN", "China", "AS");
      }
      else if (uid % NB_NATION == 5)
      {
         return sql_parser_add_nation(uid, "JP", "Japan", "AS");
      }
   }
   else if (NB_LINEITEMS + NB_ORDERS + NB_CUSTOMER + NB_NATION <= uid && uid < NB_LINEITEMS + NB_ORDERS + NB_CUSTOMER + NB_NATION + NB_REGION)
   {
      if (uid % NB_REGION == 0)
      {
         return sql_parser_add_region(uid, "NA", "NorthAmerica");
      }
      else if (uid % NB_REGION == 1)
      {
         return sql_parser_add_region(uid, "EU", "Europe");
      }
      else if (uid % NB_REGION == 2)
      {
         return sql_parser_add_region(uid, "AS", "Asia");
      }
   }

   printf("return null\n");
   return NULL;
}


size_t get_db_size_sql_parser(void) {
   return NB_LINEITEMS + NB_ORDERS + NB_CUSTOMER + NB_NATION + NB_REGION;
}


/*
 * Generic callback helpers. Allow to automatically perform the multiple steps of a SQL PARSER query without dealing too much with asynchrony.
 */
static volatile uint64_t failed_trans, nb_commits_done;
static void commit_cb(struct slab_callback *cb, void *item) {
   uint64_t end;
   rdtscll(end);
   add_timing_stat(end - get_start_time(cb->transaction));

   __sync_fetch_and_add(&nb_commits_done, 1);
   if(has_failed(cb->transaction))
      __sync_fetch_and_add(&failed_trans, 1);

   free(get_payload(cb->transaction));
   free(cb);
}

struct transaction_payload {
   uint64_t query_id;
   uint64_t nb_requests_to_do, completed_requests; // when completed_requests == nb_requests_to_do, commit transaction
};

static void compute_stats_sql_parser(struct slab_callback *cb, void *item) {
   struct transaction_payload *p = get_payload(cb->transaction);

   slab_cb_t *next = cb->payload2;
   if(!has_failed(cb->transaction)) {
      next(cb, item);
   } else {
      //printf("%p\n", next);
      free(cb->payload);
   }

   p->completed_requests++;
   if(p->completed_requests == p->nb_requests_to_do) { // Time to commit
      struct slab_callback *new_cb = new_slab_callback();
      new_cb->cb = commit_cb;
      new_cb->transaction = cb->transaction;
      new_cb->injector_queue = cb->injector_queue;
      kv_commit(cb->transaction, new_cb);
   }

   free(cb->item);
   free(cb);
}

static struct transaction_payload *get_sql_parser_payload(struct transaction *t) {
   return get_payload(t);
}

static struct slab_callback *sql_parser_cb(struct injector_queue *q, struct transaction *t, void *payload, slab_cb_t *next) {
   struct slab_callback *cb = new_slab_callback();
   cb->cb = compute_stats_sql_parser;
   cb->injector_queue = q;
   cb->payload = payload;
   cb->payload2 = next;
   get_sql_parser_payload(t)->nb_requests_to_do++;
   return cb;
}

struct injector_bg_work {
   struct slab_callback *scan_cb;
   int nb_scans_done;
   int uid;
   struct_declare_timer;
};

struct scan_payload {
   size_t nb_scans;
   ht* map;
   volatile int done;
};

static size_t scan_progress(struct slab_callback *scan_cb) {
   if(!scan_cb)
      return 0;

   struct scan_payload *p = scan_cb->payload;
   return p->nb_scans;
}

static int is_scan_complete(struct slab_callback *scan_cb) {
   if(!scan_cb)
      return 0;

   struct scan_payload *p = scan_cb->payload;
   if(p->done == 1) {
      p->done = 2;
      return 1;
   }
   return 0;
}

#define NB_BG_SCANS 1
static volatile int glob_nb_scans_done;

static void scan_map(struct slab_callback *cb, void *item) {
   struct scan_payload *p = cb->payload;
   __sync_fetch_and_add(&p->nb_scans, 1);
   if (!item) {
      p->done = 1;
      // printf("scan_map item is null \n");
   } 
   else {
      // printf("item value is %ld \n", get_shash_uint(item, 3));
      long item_uid = get_shash_uint(item, *((int *) ht_get(lineitem_table->column_map, "LINENUMBER")));
      long item_quantity = get_shash_uint(item, *((int *) ht_get(lineitem_table->column_map, "QUANTITY")));
      char* item_returnflag = get_shash_string(item, *((int *) ht_get(lineitem_table->column_map, "RETURNFLAG")));
      char* item_linestatus = get_shash_string(item, *((int *) ht_get(lineitem_table->column_map, "LINESTATUS")));
      char* item_shipdate = get_shash_string(item, *((int *) ht_get(lineitem_table->column_map, "SHIPDATE")));

      if (strcmp(item_shipdate, "1998-09-04") < 0 || strcmp(item_shipdate, "1998-09-04") == 0) {         
         char *key = "";
         asprintf(&key, "%s|%s", item_returnflag, item_linestatus);
         int *quantity_sum = (int *) ht_get(p->map, key);

         if (quantity_sum == NULL) {
            quantity_sum = malloc(sizeof(int));
            *quantity_sum = 0; 
         }

         *quantity_sum += item_quantity;
         ht_set(p->map, key, quantity_sum);

         // __sync_fetch_and_add(&p->quantity_sum[item_linestatus%3], get_shash_uint(item, 4));
         // printf("calling scan_map\n");
         // dump_shash(item);
         // printf("linestatus is %ld, quantity sum is %ld \n", item_linestatus, p->quantity_sum[item_uid%3]);
      }
   }
}

static void simple_scan_map(struct slab_callback *cb, void *item) {
   struct scan_payload *p = cb->payload;
   __sync_fetch_and_add(&p->nb_scans, 1);
   if (!item) {
      p->done = 1;
   }
   else {
      // printf("-----------------------------------\n");
      // dump_shash(item);

      if (query == NULL)
      {
         return;
      }

      table_t *table_info = lineitem_table;
      if (strcmp(query->table_name_ptr->name, "lineitem") == 0)
      {
         table_info = lineitem_table;
      }
      else if (strcmp(query->table_name_ptr->name, "orders") == 0)
      {
         table_info = orders_table;
      }
      else if (strcmp(query->table_name_ptr->name, "customer") == 0)
      {
         table_info = customer_table;
      }
      else if (strcmp(query->table_name_ptr->name, "nation") == 0)
      {
         table_info = nation_table;
      }
      else if (strcmp(query->table_name_ptr->name, "region") == 0)
      {
         table_info = region_table;
      }

      bool valid_conds = true;
      // check if item respect the where condition
      condition_t *current_condition = query->condition_ptr;      
      if (current_condition != NULL) {
         bool b = evaluate_where_condition(item, current_condition->operand1->name, current_condition, table_info);
         valid_conds = valid_conds && b;
      }

      // printf("pass the first condition check \n");
      current_condition = query->and_condition_ptr;
      while (current_condition != NULL)
      {
         bool b = evaluate_where_condition(item, current_condition->operand1->name, current_condition, table_info);
         valid_conds = valid_conds && b;
         if (!valid_conds) 
         {
            break;
         }
         current_condition = current_condition->next_condition;
      }

      // printf("pass the and condition check \n");
      current_condition = query->or_condition_ptr;
      while (current_condition != NULL)
      {
         bool b = evaluate_where_condition(item, current_condition->operand1->name, current_condition, table_info);
         valid_conds = valid_conds || b;

         if (valid_conds) {
            break;
         }
         
         current_condition = current_condition->next_condition;
      }

      // printf("pass the or condition check \n");
      if (!valid_conds) {
         return;
      }

      // item pass the conditions check
      char *item_table = get_shash_string(item, 0);
      if (strcmp(item_table, "lineitem") == 0) 
      {
         char *key_string = get_column_string_value(item, "LINENUMBER", table_info);

         list_node_t* current_field = query->field_ptr;
         char *value = malloc(1024);
         while (current_field != NULL)
         {
            strcat(value, current_field->val);
            strcat(value, ": ");
            strcat(value, get_column_string_value(item, current_field->val, table_info));
            strcat(value, ", ");
            
            current_field = current_field->next;
         }
         // printf("%s", value);
         // printf("\n");

         ht_set(p->map, key_string, value);

         if (lineitem_result_list == NULL)
         {
            lineitem_result_list = (sql_result_node_t *) calloc(1, sizeof(sql_result_node_t));
            cur_lineitem_result_item = lineitem_result_list;
         }
         else 
         {
            cur_lineitem_result_item->next = (sql_result_node_t *) calloc(1, sizeof(sql_result_node_t));
            cur_lineitem_result_item = cur_lineitem_result_item->next;
         }
         cur_lineitem_result_item->item = item;
      }
      else if (strcmp(item_table, "orders") == 0) 
      {
         char *key_string = get_column_string_value(item, "ORDERKEY", table_info);

         list_node_t* current_field = query->field_ptr;
         char *value = malloc(1024);
         while (current_field != NULL)
         {
            strcat(value, current_field->val);
            strcat(value, ": ");
            strcat(value, get_column_string_value(item, current_field->val, table_info));
            strcat(value, ", ");
            
            current_field = current_field->next;
         }
         // printf("%s", value);
         // printf("\n");

         ht_set(p->map, key_string, value);
         
         if (orders_result_list == NULL)
         {
            orders_result_list = (sql_result_node_t *) calloc(1, sizeof(sql_result_node_t));
            cur_orders_result_item = orders_result_list;
         }
         else
         {
            cur_orders_result_item->next = (sql_result_node_t *) calloc(1, sizeof(sql_result_node_t));
            cur_orders_result_item = cur_orders_result_item->next;
         }

         cur_orders_result_item->item = item;
      }
      else if (strcmp(item_table, "customer") == 0) 
      {
         char *key_string = get_column_string_value(item, "CUSTKEY", table_info);

         list_node_t* current_field = query->field_ptr;
         char *value = malloc(1024);
         while (current_field != NULL)
         {
            strcat(value, current_field->val);
            strcat(value, ": ");
            strcat(value, get_column_string_value(item, current_field->val, table_info));
            strcat(value, ", ");
            
            current_field = current_field->next;
         }
         // printf("%s", value);
         // printf("\n");

         ht_set(p->map, key_string, value);
         
         if (customer_result_list == NULL)
         {
            customer_result_list = (sql_result_node_t *) calloc(1, sizeof(sql_result_node_t));
            cur_customer_result_item = customer_result_list;
         }
         else
         {
            cur_customer_result_item->next = (sql_result_node_t *) calloc(1, sizeof(sql_result_node_t));
            cur_customer_result_item = cur_customer_result_item->next;
         }

         cur_customer_result_item->item = item;
      }
      else if (strcmp(item_table, "nation") == 0) 
      {
         char *key_string = get_column_string_value(item, "NATIONKEY", table_info);

         list_node_t* current_field = query->field_ptr;
         char *value = malloc(1024);
         while (current_field != NULL)
         {
            strcat(value, current_field->val);
            strcat(value, ": ");
            strcat(value, get_column_string_value(item, current_field->val, table_info));
            strcat(value, ", ");
            
            current_field = current_field->next;
         }
         // printf("%s", value);
         // printf("\n");

         ht_set(p->map, key_string, value);
         
         if (nation_result_list == NULL)
         {
            nation_result_list = (sql_result_node_t *) calloc(1, sizeof(sql_result_node_t));
            cur_nation_result_item = nation_result_list;
         }
         else
         {
            cur_nation_result_item->next = (sql_result_node_t *) calloc(1, sizeof(sql_result_node_t));
            cur_nation_result_item = cur_nation_result_item->next;
         }

         cur_nation_result_item->item = item;
      }
      else if (strcmp(item_table, "region") == 0) 
      {
         // printf("-----------------------------------\n");
         // dump_shash(item);
         char *key_string = get_column_string_value(item, "REGIONKEY", table_info);

         list_node_t* current_field = query->field_ptr;
         char *value = malloc(1024);
         while (current_field != NULL)
         {
            strcat(value, current_field->val);
            strcat(value, ": ");
            strcat(value, get_column_string_value(item, current_field->val, table_info));
            strcat(value, ", ");
            
            current_field = current_field->next;
         }
         // printf("%s", value);
         // printf("\n");

         ht_set(p->map, key_string, value);
         
         if (region_result_list == NULL)
         {
            region_result_list = (sql_result_node_t *) calloc(1, sizeof(sql_result_node_t));
            cur_region_result_item = region_result_list;
         }
         else
         {
            cur_region_result_item->next = (sql_result_node_t *) calloc(1, sizeof(sql_result_node_t));
            cur_region_result_item = cur_region_result_item->next;
         }

         cur_region_result_item->item = item;
      }
   }
}

static struct slab_callback *start_background_scan(size_t uid, struct workload *w, struct injector_queue *q, size_t beginning, size_t end) {
   struct scan_payload *p = calloc(1, sizeof(*p));
   struct slab_callback *scan_cb = new_slab_callback();

   p->map = ht_create();

   // scan_cb->cb = scan_map;
   scan_cb->cb = simple_scan_map;
   scan_cb->payload = p;
   scan_cb->injector_queue = q;
   scan_cb->item = create_key(beginning);
   scan_cb->max_next_key = end;
   kv_long_scan(scan_cb); // Start a background scan!
   return scan_cb;
}

static void do_injector_bg_work(struct workload *w, struct injector_queue *q, struct injector_bg_work *p) {
   injector_process_queue(q);
   if(is_scan_complete(p->scan_cb)) {
      struct_stop_timer(p, "Scanning #%d %lu/%lu elements", p->nb_scans_done, scan_progress(p->scan_cb), get_database_size());

      struct scan_payload *returned_payload = p->scan_cb->payload;
      printf("done scanning\n"); 

      // hti iterator = ht_iterator(returned_payload->map);
      // while (ht_next(&iterator)) {
      //    printf("%s -> %s \n", iterator.key, iterator.value);
      // }

      p->nb_scans_done++;
      size_t beginning = item_get_key(p->scan_cb->item), end;
      if(glob_nb_scans_done == 0)
         end = beginning + 2000000LU;
      else
         end = p->scan_cb->max_next_key;
      free(p->scan_cb->payload);
      free(p->scan_cb->item);
      free(p->scan_cb);
      p->scan_cb = NULL;
      __sync_add_and_fetch(&glob_nb_scans_done, 1);
      if(p->nb_scans_done < NB_BG_SCANS) {
         struct_start_timer(p);
         p->scan_cb = start_background_scan(p->uid, w, q, beginning, end);
      }
   }
}

bool is_in_list(list_node_t *list_node, list_node_t *node)
{
   list_node_t *cur = list_node;
   while (cur != NULL)
   {  
      // printf("is_in_list： %s, %s \n", cur->val, node->val);
      if (strcmp(cur->val, node->val) == 0 && strcmp(cur->key, node->key) == 0)
      {
         return true;
      }
      cur = cur->next;
   }
   return false;
}

void add_str_to_set(hashset_t set, char *str)
{
   bool has_value = false;
   hashset_itr_t iter = hashset_iterator(set);
   
   while(hashset_iterator_has_next(iter))
   {
      if (strcmp((char *) hashset_iterator_value(iter), str) == 0)
      {
         has_value = true;
         break;
      }
      hashset_iterator_next(iter);
   }

   if (!has_value)
   {
      hashset_add(set, str);
   }
}

static int injector_uid;

static void _launch_sql_parser(struct workload *w, int test, int nb_requests, int zipfian) {
   struct injector_queue *q = create_new_injector_queue();
   int uid = __sync_fetch_and_add(&injector_uid, 1);

   struct injector_bg_work _p = { .uid = uid };
   struct_start_timer(&_p);

   declare_periodic_count;
   size_t nb_queries = 0;

   // query = ht_get(sub_queries_map, "l");
   // print_query_object(query);
   // _p.scan_cb = start_background_scan(uid, w, q, sql_parser_get_key_lineitem(lineitem_table->start_index), sql_parser_get_key_lineitem(lineitem_table->end_index));
   
   // do {
   //    // periodic_count(1000, "TRANS Load Injector %d [failure rate = %lu%%] [snapshot size %lu] [running transactions %lu] [%lu scans done - %d full] [%lu queries done]", uid, failed_trans*100/nb_commits_done, get_snapshot_size(), get_nb_running_transactions(), scan_progress(_p.scan_cb), glob_nb_scans_done, nb_queries);
   //    do_injector_bg_work(w, q, &_p);
   // } while(pending_work() || injector_has_pending_work(q));

   // query = ht_get(sub_queries_map, "o");
   // print_query_object(query);
   // _p.scan_cb = start_background_scan(uid, w, q, sql_parser_get_key_order(orders_table->start_index), sql_parser_get_key_order(orders_table->end_index));

   // do {
   //    // periodic_count(1000, "TRANS Load Injector %d [failure rate = %lu%%] [snapshot size %lu] [running transactions %lu] [%lu scans done - %d full] [%lu queries done]", uid, failed_trans*100/nb_commits_done, get_snapshot_size(), get_nb_running_transactions(), scan_progress(_p.scan_cb), glob_nb_scans_done, nb_queries);
   //    do_injector_bg_work(w, q, &_p);
   // } while(pending_work() || injector_has_pending_work(q));

   hti sub_queries_map_iterator = ht_iterator(sub_queries_map);
   while (ht_next(&sub_queries_map_iterator))
   {
      char *table_identifier = (char *) sub_queries_map_iterator.key;
      query = (query_t *) sub_queries_map_iterator.value;
      
      printf("\nThe query object for table %s\n", table_identifier);
      print_query_object(query);

      long start_index = 0;
      long end_index = 0;
      if (strcmp(query->table_name_ptr->name, "lineitem") == 0)
      {
         start_index = sql_parser_get_key_lineitem(lineitem_table->start_index);
         end_index = sql_parser_get_key_lineitem(lineitem_table->end_index);
      }
      else if (strcmp(query->table_name_ptr->name, "orders") == 0)
      {
         start_index = sql_parser_get_key_order(orders_table->start_index);
         end_index = sql_parser_get_key_order(orders_table->end_index);
      }
      else if (strcmp(query->table_name_ptr->name, "customer") == 0)
      {
         start_index = sql_parser_get_key_customer(customer_table->start_index);
         end_index = sql_parser_get_key_customer(customer_table->end_index);
      }
      else if (strcmp(query->table_name_ptr->name, "nation") == 0)
      {
         start_index = sql_parser_get_key_nation(nation_table->start_index);
         end_index = sql_parser_get_key_nation(nation_table->end_index);
      }
      else if (strcmp(query->table_name_ptr->name, "region") == 0)
      {
         start_index = sql_parser_get_key_region(region_table->start_index);
         end_index = sql_parser_get_key_region(region_table->end_index);
      }

      _p.scan_cb = start_background_scan(uid, w, q, start_index, end_index);
   
      do {
         // periodic_count(1000, "TRANS Load Injector %d [failure rate = %lu%%] [snapshot size %lu] [running transactions %lu] [%lu scans done - %d full] [%lu queries done]", uid, failed_trans*100/nb_commits_done, get_snapshot_size(), get_nb_running_transactions(), scan_progress(_p.scan_cb), glob_nb_scans_done, nb_queries);
         do_injector_bg_work(w, q, &_p);
      } while(pending_work() || injector_has_pending_work(q));
   }

   sql_result_node_t *outcome_list = NULL;
   sql_result_node_t *cur_outcome_node = NULL;
   table_t *outcome_table;

   sql_result_node_t *intermediate_node = NULL;
   sql_result_node_t *current_intermediate_node = NULL;
   
   table_t *intermediate_table = (table_t *) calloc(1, sizeof(table_t));
   intermediate_table->column_map = ht_create();
   int tmp_column_index = 0;
   long tmp_item_index = 0;

   condition_t *condition = origin_query->condition_ptr;
   while (condition != NULL)
   {
      if (condition->is_join_condition)
      {
         field_operand_t *field_operand = condition->operand1;
         comparison_condition_t *comparison_condition = (comparison_condition_t *) condition->operand2;

         char *table1_name = ht_get(table_identifier_to_table_name, field_operand->table_identifier);
         char *field1_name = field_operand->name;
         
         char *table2_name = ht_get(table_identifier_to_table_name, comparison_condition->table);
         char *field2_name = (char *) comparison_condition->value;

         sql_result_node_t *cur1_result_node;
         table_t *table1;
         if (strcmp(table1_name, "lineitem") == 0)
         {
            cur1_result_node = lineitem_result_list;
            table1 = lineitem_table;
         }
         else if (strcmp(table1_name, "orders") == 0)
         {
            cur1_result_node = orders_result_list;
            table1 = orders_table;
         }
         else if (strcmp(table1_name, "customer") == 0)
         {
            cur1_result_node = customer_result_list;
            table1 = customer_table;
         }
         else if (strcmp(table1_name, "nation") == 0)
         {
            cur1_result_node = nation_result_list;
            table1 = nation_table;
         }
         else if (strcmp(table1_name, "region") == 0)
         {
            cur1_result_node = region_result_list;
            table1 = region_table;
         }

         hti table1_iterator = ht_iterator(table1->column_map);
         while (ht_next(&table1_iterator)) 
         {
            if (ht_get(intermediate_table->column_map, table1_iterator.key) == NULL)
            {
               struct column_info *tmp_ci = calloc(1, sizeof(tmp_ci));
               tmp_ci->index = tmp_column_index;
               tmp_ci->type = ((struct column_info *) (table1_iterator.value))->type;
               tmp_column_index++;
               ht_set(intermediate_table->column_map, table1_iterator.key, tmp_ci);
            }
         }
         bool added = false;

         while (cur1_result_node != NULL)
         {
            sql_result_node_t *cur2_result_node;
            table_t *table2;
            if (strcmp(table2_name, "lineitem") == 0)
            {
               cur2_result_node = lineitem_result_list;
               table2 = lineitem_table;
            }
            else if (strcmp(table2_name, "orders") == 0)
            {
               cur2_result_node = orders_result_list;
               table2 = orders_table;
            }
            else if (strcmp(table2_name, "customer") == 0)
            {
               cur2_result_node = customer_result_list;
               table2 = customer_table;
            }
            else if (strcmp(table2_name, "nation") == 0)
            {
               cur2_result_node = nation_result_list;
               table2 = nation_table;
            }
            else if (strcmp(table2_name, "region") == 0)
            {
               cur2_result_node = region_result_list;
               table2 = region_table;
            }

            if (!added)
            {
               hti table2_iterator = ht_iterator(table2->column_map);
               while (ht_next(&table2_iterator)) 
               {
                  if (ht_get(intermediate_table->column_map, table2_iterator.key) == NULL)
                  {
                     struct column_info *tmp_ci = calloc(1, sizeof(tmp_ci));
                     tmp_ci->index = tmp_column_index;
                     tmp_ci->type = ((struct column_info *) (table2_iterator.value))->type;
                     tmp_column_index++;
                     ht_set(intermediate_table->column_map, table2_iterator.key, tmp_ci);
                  }
               }
               added = true;
            }
            
            while (cur2_result_node != NULL) 
            {
               char *value2 = get_column_string_value(cur2_result_node->item, field2_name, table2);
               bool valid = compare_column_value(cur1_result_node->item, field1_name, condition->operator, value2, false, table1);
               
               if (valid)
               {
                  char *tmp_item = sql_parser_tmp(tmp_item_index);
                  tmp_item_index++;

                  hti table1_iterator = ht_iterator(table1->column_map);
                  while (ht_next(&table1_iterator)) 
                  {
                     if (strcmp(table1_iterator.key, "TABLE") != 0)
                     {
                        char *tmp_value = get_column_string_value(cur1_result_node->item, table1_iterator.key, table1);
                        struct column_info *ci = ht_get(intermediate_table->column_map, table1_iterator.key);
                        switch (ci->type)
                        {
                        case INT:
                           tmp_item = add_column_value(tmp_item, table1_iterator.key, atoi(tmp_value), intermediate_table);
                           break;
                        case STRING:
                           tmp_item = add_column_value(tmp_item, table1_iterator.key, tmp_value, intermediate_table);
                           break;
                        default:
                           break;
                        }
                     }
                  }

                  hti table2_iterator = ht_iterator(table2->column_map);
                  while (ht_next(&table2_iterator)) 
                  {
                     if (strcmp(table2_iterator.key, field2_name) != 0 && strcmp(table2_iterator.key, "TABLE") != 0)
                     {
                        char *tmp_value = get_column_string_value(cur2_result_node->item, table2_iterator.key, table2);
                        struct column_info *ci = ht_get(intermediate_table->column_map, table2_iterator.key);
                        switch (ci->type)
                        {
                        case INT:
                           tmp_item = add_column_value(tmp_item, table2_iterator.key, atoi(tmp_value), intermediate_table);
                           break;
                        case STRING:
                           tmp_item = add_column_value(tmp_item, table2_iterator.key, tmp_value, intermediate_table);
                           break;
                        default:
                           break;
                        }
                     }
                  }

                  if (intermediate_node == NULL)
                  {
                     intermediate_node = (sql_result_node_t *) calloc(1, sizeof(sql_result_node_t));
                     current_intermediate_node = intermediate_node;
                  }
                  else 
                  {
                     current_intermediate_node->next = (sql_result_node_t *) calloc(1, sizeof(sql_result_node_t));
                     current_intermediate_node = current_intermediate_node->next;
                  }
                  current_intermediate_node->item = tmp_item;
               }

               cur2_result_node = cur2_result_node->next;
            }

            cur1_result_node = cur1_result_node->next;
         }
      }

      condition = condition->next_condition;
   }
   printf("\n");
   
   tmp_column_index = 0;
   if (intermediate_node != NULL)
   {
      outcome_list = intermediate_node;
      outcome_table = intermediate_table;

      intermediate_node = NULL;
      current_intermediate_node = NULL;
      intermediate_table = (table_t *) calloc(1, sizeof(table_t));
      intermediate_table->column_map = ht_create();
   }
   

   condition = origin_query->and_condition_ptr;
   while (condition != NULL)
   {
      if (condition->is_join_condition)
      {
         field_operand_t *field_operand = condition->operand1;
         comparison_condition_t *comparison_condition = (comparison_condition_t *) condition->operand2;

         char *table1_name = ht_get(table_identifier_to_table_name, field_operand->table_identifier);
         char *field1_name = field_operand->name;
         
         char *table2_name = ht_get(table_identifier_to_table_name, comparison_condition->table);
         char *field2_name = (char *) comparison_condition->value;
         // printf("%s_%s = %s_%s\n", table1_name, field1_name, table2_name, field2_name);

         sql_result_node_t *cur1_result_node;
         table_t *table1;
         if (outcome_list != NULL && ht_get(outcome_table->column_map, field1_name) != NULL) 
         {
            // printf("in outcome_table \n");
            cur1_result_node = outcome_list;
            table1 = outcome_table;
         }
         else if (strcmp(table1_name, "lineitem") == 0)
         {
            cur1_result_node = lineitem_result_list;
            table1 = lineitem_table;
         }
         else if (strcmp(table1_name, "orders") == 0)
         {
            cur1_result_node = orders_result_list;
            table1 = orders_table;
         }
         else if (strcmp(table1_name, "customer") == 0)
         {
            cur1_result_node = customer_result_list;
            table1 = customer_table;
         }
         else if (strcmp(table1_name, "nation") == 0)
         {
            cur1_result_node = nation_result_list;
            table1 = nation_table;
         }
         else if (strcmp(table1_name, "region") == 0)
         {
            cur1_result_node = region_result_list;
            table1 = region_table;
         }

         hti table1_iterator = ht_iterator(table1->column_map);
         while (ht_next(&table1_iterator)) 
         {
            if (ht_get(intermediate_table->column_map, table1_iterator.key) == NULL)
            {
               struct column_info *tmp_ci = calloc(1, sizeof(tmp_ci));
               tmp_ci->index = tmp_column_index;
               tmp_ci->type = ((struct column_info *) (table1_iterator.value))->type;
               tmp_column_index++;
               ht_set(intermediate_table->column_map, table1_iterator.key, tmp_ci);
            }
         }
         bool added = false;

         while (cur1_result_node != NULL)
         {
            // printf("--------------cur1_result_node---------------\n");
            // dump_shash(cur1_result_node->item);
            sql_result_node_t *cur2_result_node;
            table_t *table2;
            if (strcmp(table2_name, "lineitem") == 0)
            {
               cur2_result_node = lineitem_result_list;
               table2 = lineitem_table;
            }
            else if (strcmp(table2_name, "orders") == 0)
            {
               cur2_result_node = orders_result_list;
               table2 = orders_table;
            }
            else if (strcmp(table2_name, "customer") == 0)
            {
               cur2_result_node = customer_result_list;
               table2 = customer_table;
            }
            else if (strcmp(table2_name, "nation") == 0)
            {
               cur2_result_node = nation_result_list;
               table2 = nation_table;
            }
            else if (strcmp(table2_name, "region") == 0)
            {
               cur2_result_node = region_result_list;
               table2 = region_table;
            }
            else if (outcome_list != NULL && ht_get(outcome_table->column_map, field2_name) != NULL)
            {
               cur2_result_node = outcome_list;
               table2 = outcome_table;
            }
            
            if (!added)
            {
               hti table2_iterator = ht_iterator(table2->column_map);
               while (ht_next(&table2_iterator)) 
               {
                  if (ht_get(intermediate_table->column_map, table2_iterator.key) == NULL)
                  {
                     struct column_info *tmp_ci = calloc(1, sizeof(tmp_ci));
                     tmp_ci->index = tmp_column_index;
                     tmp_ci->type = ((struct column_info *) (table2_iterator.value))->type;
                     tmp_column_index++;
                     ht_set(intermediate_table->column_map, table2_iterator.key, tmp_ci);
                  }
               }
               added = true;
            }
            
            while (cur2_result_node != NULL) 
            {
               char *value2 = get_column_string_value(cur2_result_node->item, field2_name, table2);
               bool valid = compare_column_value(cur1_result_node->item, field1_name, condition->operator, value2, false, table1);

               if (valid)
               {
                  char *tmp_item = sql_parser_tmp(tmp_item_index);
                  tmp_item_index++;

                  hti table1_iterator = ht_iterator(table1->column_map);
                  while (ht_next(&table1_iterator)) 
                  {
                     if (strcmp(table1_iterator.key, "TABLE") != 0)
                     {
                        char *tmp_value = get_column_string_value(cur1_result_node->item, table1_iterator.key, table1);
                        struct column_info *ci = ht_get(intermediate_table->column_map, table1_iterator.key);
                        switch (ci->type)
                        {
                        case INT:
                           tmp_item = add_column_value(tmp_item, table1_iterator.key, atoi(tmp_value), intermediate_table);
                           break;
                        case STRING:
                           tmp_item = add_column_value(tmp_item, table1_iterator.key, tmp_value, intermediate_table);
                           break;
                        default:
                           break;
                        }
                     }
                  }

                  hti table2_iterator = ht_iterator(table2->column_map);
                  while (ht_next(&table2_iterator)) 
                  {
                     if (strcmp(table2_iterator.key, field2_name) != 0 && strcmp(table2_iterator.key, "TABLE") != 0)
                     {
                        char *tmp_value = get_column_string_value(cur2_result_node->item, table2_iterator.key, table2);
                        struct column_info *ci = ht_get(intermediate_table->column_map, table2_iterator.key);
                        switch (ci->type)
                        {
                        case INT:
                           tmp_item = add_column_value(tmp_item, table2_iterator.key, atoi(tmp_value), intermediate_table);
                           break;
                        case STRING:
                           tmp_item = add_column_value(tmp_item, table2_iterator.key, tmp_value, intermediate_table);
                           break;
                        default:
                           break;
                        }
                     }
                  }

                  if (intermediate_node == NULL)
                  {
                     intermediate_node = (sql_result_node_t *) calloc(1, sizeof(sql_result_node_t));
                     current_intermediate_node = intermediate_node;
                  }
                  else 
                  {
                     current_intermediate_node->next = (sql_result_node_t *) calloc(1, sizeof(sql_result_node_t));
                     current_intermediate_node = current_intermediate_node->next;
                  }
                  current_intermediate_node->item = tmp_item;
               }

               cur2_result_node = cur2_result_node->next;
            }

            cur1_result_node = cur1_result_node->next;
         }
      }
      
      condition = condition->next_condition;
      if (condition != NULL)
      {
         tmp_column_index = 0;
      }

      // if (intermediate_node == NULL) {
      //    printf("yes intermediate_node is null\n");
      // }
      // else {
      //    printf("yes intermediate_node is not null\n");
      // }

      if (intermediate_node != NULL)
      {
         // hti intermediate_table_iterator = ht_iterator(intermediate_table->column_map);
         // while (ht_next(&intermediate_table_iterator)) 
         // {
         //    printf("%s\n", (char *)intermediate_table_iterator.key);
         // }

         outcome_list = intermediate_node;
         outcome_table = intermediate_table;

         intermediate_node = NULL;
         current_intermediate_node = NULL;
         intermediate_table = (table_t *) calloc(1, sizeof(table_t));
         intermediate_table->column_map = ht_create();
      }
   }

   // printf("-----------------------\n");
   // hti outcome_table_iterator = ht_iterator(outcome_table->column_map);
   // while (ht_next(&outcome_table_iterator)) 
   // {
   //    printf("%s\n", (char *)outcome_table_iterator.key);
   // }

   if (outcome_list == NULL)
   {
      if (strcmp(origin_query->table_name_ptr->name, "lineitem") == 0) 
      {
         outcome_list = lineitem_result_list;
         outcome_table = lineitem_table;
      }
      else if (strcmp(origin_query->table_name_ptr->name, "orders") == 0)
      {
         outcome_list = orders_result_list;
         outcome_table = orders_table;
      }
      else if (strcmp(origin_query->table_name_ptr->name, "customer") == 0)
      {
         outcome_list = customer_result_list;
         outcome_table = customer_table;
      }
      else if (strcmp(origin_query->table_name_ptr->name, "nation") == 0)
      {
         outcome_list = nation_result_list;
         outcome_table = nation_table;
      }
      else if (strcmp(origin_query->table_name_ptr->name, "nation") == 0)
      {
         outcome_list = region_result_list;
         outcome_table = region_table;
      }
   }

   printf("final outcome\n");
   long total_num_item = 0;
   cur_outcome_node = outcome_list;
   while (cur_outcome_node != NULL)
   {
      list_node_t *cur_field_ptr = origin_query->field_ptr;
      while (cur_field_ptr != NULL)
      {
         if (cur_field_ptr->value_type == COLUMN_FIELD)
         {
            char *outcome_value = get_column_string_value(cur_outcome_node->item, cur_field_ptr->val, outcome_table);
            printf("%s: %s, ", cur_field_ptr->val, outcome_value);
         }
         cur_field_ptr = cur_field_ptr->next;
      }
      printf("\n");
      cur_outcome_node = cur_outcome_node->next;

      total_num_item++;
   }

   printf("\nafter grouping\n");
   hashset_t group_by_key_set = hashset_create();

   ht *group_by_result_map;
   if (origin_query->group_by_ptr != NULL) 
   {
      // grouping
      group_by_result_map = ht_create();
      cur_outcome_node = outcome_list;
      while (cur_outcome_node != NULL)
      {
         char *group_by_key = "";
         group_by_node_t *cur_group_by_column = origin_query->group_by_ptr;
         while (cur_group_by_column != NULL)
         {
            asprintf(&group_by_key, "%s_%s", group_by_key, get_column_string_value(cur_outcome_node->item, cur_group_by_column->val, outcome_table));
            cur_group_by_column = cur_group_by_column->next;
         }
         
         group_by_t *group_by_result = (group_by_t *) ht_get(group_by_result_map, group_by_key);
         if (group_by_result == NULL)
         {
            group_by_result = (group_by_t *) calloc(1, sizeof(group_by_t));
            group_by_result->item_array = (char **) calloc(1, sizeof(char) * total_num_item);
            group_by_result->last_index = 0;
         }

         group_by_result->item_array[group_by_result->last_index] = cur_outcome_node->item;
         group_by_result->last_index += 1;

         ht_set(group_by_result_map, group_by_key, group_by_result);
         add_str_to_set(group_by_key_set, group_by_key);
         cur_outcome_node = cur_outcome_node->next;
      }

      // for debugging
      // hashset_itr_t group_by_key_set_iterator = hashset_iterator(group_by_key_set);
      // while (hashset_iterator_has_next(group_by_key_set_iterator))
      // {
      //    char *group_by_key = (char *) hashset_iterator_value(group_by_key_set_iterator);
      //    group_by_t *group_by_result = (group_by_t *) ht_get(group_by_result_map, group_by_key);

      //    printf("%s: %d items \n", group_by_key, group_by_result->last_index);
      //    hashset_iterator_next(group_by_key_set_iterator);
      // }

      // adding sum and avg to the column table
      list_node_t *cur_field_ptr = origin_query->field_ptr;
      while (cur_field_ptr != NULL)
      {
         switch (cur_field_ptr->value_type)
         {
            case SUM:
            {
               struct column_info *sum_ci = calloc(1, sizeof(sum_ci));
               sum_ci->index = tmp_column_index;
               sum_ci->type = INT;
               char *sum_colum;

               if (cur_field_ptr->as != NULL)
               {
                  sum_colum = cur_field_ptr->as;
               }
               else 
               {
                  sum_colum = "";
                  asprintf(&sum_colum, "SUM(%s)", cur_field_ptr->val);
               }
               
               ht_set(outcome_table->column_map, sum_colum, sum_ci);
               tmp_column_index++;
               break;
            }
            case AVG:
            {
               struct column_info *avg_ci = calloc(1, sizeof(avg_ci));
               avg_ci->index = tmp_column_index;
               avg_ci->type = INT;
               char *avg_colum;

               if (cur_field_ptr->as != NULL)
               {
                  avg_colum = cur_field_ptr->as;
               }
               else
               {
                  avg_colum = "";
                  asprintf(&avg_colum, "AVG(%s)", cur_field_ptr->val);
               }
               
               ht_set(outcome_table->column_map, avg_colum, avg_ci);
               tmp_column_index++;
               break;
            }
            default:
               break;
         } 

         cur_field_ptr = cur_field_ptr->next;
      }

      // hti outcome_table_iterator = ht_iterator(outcome_table->column_map);
      // while (ht_next(&outcome_table_iterator)) 
      // {
      //    printf("%s\n", (char *)outcome_table_iterator.key);
      // }

      // create group_outcome_list;
      sql_result_node_t *group_by_outcome_list = NULL;
      sql_result_node_t *current_group_by_node = NULL;
      table_t *group_by_table = (table_t *) calloc(1, sizeof(table_t));
      group_by_table->column_map = ht_create();
      int group_by_index = 0;

      // hti group_by_result_map_iterator = ht_iterator(group_by_result_map);
      hashset_itr_t group_by_key_set_iter = hashset_iterator(group_by_key_set);
      while (hashset_iterator_has_next(group_by_key_set_iter))
      {
         char *group_by_key = (char *) hashset_iterator_value(group_by_key_set_iter);
         group_by_t *group_by_result = (group_by_t *) ht_get(group_by_result_map, group_by_key);

         char *tmp_item = sql_parser_tmp(tmp_item_index);
         tmp_item_index++;
         list_node_t *cur_field_ptr = origin_query->field_ptr;
         while (cur_field_ptr != NULL)
         {
            switch (cur_field_ptr->value_type)
            {
               case COLUMN_FIELD:
               {
                  if (ht_get(group_by_table->column_map, cur_field_ptr->val) == NULL)
                  {
                     struct column_info *group_by_ci = calloc(1, sizeof(group_by_ci));
                     struct column_info *outcome_ci = ht_get(outcome_table->column_map, cur_field_ptr->val);
                     group_by_ci->type = outcome_ci->type;
                     group_by_ci->index = group_by_index;
                     group_by_index++;

                     ht_set(group_by_table->column_map, cur_field_ptr->val, group_by_ci);
                  }

                  char *value = get_column_string_value(group_by_result->item_array[0], cur_field_ptr->val, outcome_table);

                  struct column_info *group_by_ci = ht_get(group_by_table->column_map, cur_field_ptr->val);
                  if (group_by_ci->type == STRING)
                  {
                     tmp_item = add_column_value(tmp_item, cur_field_ptr->val, value, group_by_table);
                  }
                  else if (group_by_ci->type == INT)
                  {
                     tmp_item = add_column_value(tmp_item, cur_field_ptr->val, atoi(value), group_by_table);
                  }

                  break;
               }
               case SUM:
               {
                  char *sum_colum;
                  if (cur_field_ptr->as != NULL)
                  {
                     sum_colum = cur_field_ptr->as;
                  }
                  else
                  {
                     sum_colum = "";
                     asprintf(&sum_colum, "SUM(%s)", cur_field_ptr->val);
                  }

                  if (ht_get(group_by_table->column_map, sum_colum) == NULL)
                  {
                     struct column_info *group_by_ci = calloc(1, sizeof(group_by_ci));
                     struct column_info *outcome_ci = ht_get(outcome_table->column_map, sum_colum);
                     group_by_ci->type = outcome_ci->type;
                     group_by_ci->index = group_by_index;
                     group_by_index++;

                     ht_set(group_by_table->column_map, sum_colum, group_by_ci);
                  }

                  int sum = 0;
                  for (int i=0; i<group_by_result->last_index; i++)
                  {
                     char *value = get_column_string_value(group_by_result->item_array[i], cur_field_ptr->val, outcome_table);
                     sum += atoi(value);
                  }
               
                  tmp_item = add_column_value(tmp_item, sum_colum, sum, group_by_table);
                  break;
               }
               case AVG:
               {
                  char *avg_colum;
                  if (cur_field_ptr->as != NULL)
                  {
                     avg_colum = cur_field_ptr->as;
                  }
                  else 
                  {
                     avg_colum = "";
                     asprintf(&avg_colum, "AVG(%s)", cur_field_ptr->val);
                  }

                  if (ht_get(group_by_table->column_map, avg_colum) == NULL)
                  {
                     struct column_info *group_by_ci = calloc(1, sizeof(group_by_ci));
                     struct column_info *outcome_ci = ht_get(outcome_table->column_map, avg_colum);
                     group_by_ci->type = outcome_ci->type;
                     group_by_ci->index = group_by_index;
                     group_by_index++;

                     ht_set(group_by_table->column_map, avg_colum, group_by_ci);
                  }

                  int sum = 0;
                  for (int i=0; i<group_by_result->last_index; i++)
                  {
                     char *value = get_column_string_value(group_by_result->item_array[i], cur_field_ptr->val, outcome_table);
                     sum += atoi(value);
                  }

                  int avg = sum / group_by_result->last_index;
                  tmp_item = add_column_value(tmp_item, avg_colum, avg, group_by_table);
                  // dump_shash(tmp_item);
                  break;
               }
               default:
                  break;
            }
            cur_field_ptr = cur_field_ptr->next;
         }

         if (group_by_outcome_list == NULL)
         {
            group_by_outcome_list = (sql_result_node_t *) calloc(1, sizeof(sql_result_node_t));
            current_group_by_node = group_by_outcome_list;
         }
         else 
         {
            current_group_by_node->next = (sql_result_node_t *) calloc(1, sizeof(sql_result_node_t));
            current_group_by_node = current_group_by_node->next;
         }

         current_group_by_node->item = tmp_item;
         hashset_iterator_next(group_by_key_set_iter);
      }

      outcome_list = group_by_outcome_list;
      outcome_table = group_by_table;
   }
   else 
   {

   }

   // hti outcome_table_iterator = ht_iterator(outcome_table->column_map);
   // while (ht_next(&outcome_table_iterator)) 
   // {
   //    printf("%s\n", (char *)outcome_table_iterator.key);
   // }
   // printf("\n");
   
   // order the outcome list by order by columns
   if (origin_query->order_by_ptr != NULL)
   {
      sql_result_node_t *dummy_head = (sql_result_node_t *) calloc(1, sizeof(sql_result_node_t));
      dummy_head->next = outcome_list;

      // bubble sort
      bool to_stop;
      while (true)
      {
         to_stop = true;

         sql_result_node_t *p1 = dummy_head;
         sql_result_node_t *p2 = dummy_head->next;
         sql_result_node_t *p3 = dummy_head->next->next;
         
         while (p2 != NULL && p3 != NULL)
         {
            bool to_swap;
            order_by_node_t *current_order_by_node = origin_query->order_by_ptr;
            while (current_order_by_node != NULL)
            {
               operator_t op;
               if (current_order_by_node->desc) 
               {
                  op = LT;
               }
               else
               {
                  op = GT;
               }

               char *value_to_compare = get_column_string_value(p3->item, current_order_by_node->val, outcome_table);
               to_swap = compare_column_value(p2->item, current_order_by_node->val, op, value_to_compare, false, outcome_table);

               if (to_swap) 
               {
                  break;
               }
               else
               {
                  bool is_eq = compare_column_value(p2->item, current_order_by_node->val, EQ, value_to_compare, false, outcome_table);
                  if (!is_eq)
                  {
                     break; 
                  }
                  current_order_by_node = current_order_by_node->next;
               }
            }

            if (to_swap)
            {
               p1->next = p3;
               p2->next = p3->next;
               p3->next = p2;
               to_stop = false;
            }

            p1 = p1->next;
            p2 = p1->next;
            p3 = p2->next;
         }

         if (to_stop)
         {
            break;
         }
      }

      outcome_list = dummy_head->next;
   }
   

   cur_outcome_node = outcome_list;
   while (cur_outcome_node != NULL)
   {
      list_node_t *cur_field_ptr = origin_query->field_ptr;
      while (cur_field_ptr != NULL)
      {
         char *field;
         char *outcome_value;
         if (cur_field_ptr->value_type == COLUMN_FIELD)
         {
            field = cur_field_ptr->val;
            outcome_value = get_column_string_value(cur_outcome_node->item, cur_field_ptr->val, outcome_table);
         }
         else if (cur_field_ptr->value_type == SUM)
         {
            if (cur_field_ptr->as != NULL)
            {
               field = cur_field_ptr->as;
            }
            else
            {
               field = "";
               asprintf(&field, "SUM(%s)", cur_field_ptr->val);
            }
            
            outcome_value = get_column_string_value(cur_outcome_node->item, field, outcome_table);
         }
         else if (cur_field_ptr->value_type == AVG)
         {
            if (cur_field_ptr->as != NULL)
            {
               field = cur_field_ptr->as;
            }
            else
            {
               field = "";
               asprintf(&field, "AVG(%s)", cur_field_ptr->val);
            }
            field = "";
            asprintf(&field, "AVG(%s)", cur_field_ptr->val);
            outcome_value = get_column_string_value(cur_outcome_node->item, field, outcome_table);
         }
         
         printf("%s: %s, ", field, outcome_value);
         cur_field_ptr = cur_field_ptr->next;
      }
      printf("\n");
      cur_outcome_node = cur_outcome_node->next;
   }

   // hti group_by_result_map_iterator = ht_iterator(group_by_result_map);
   // while (ht_next(&group_by_result_map_iterator))
   // {
   //    char *group_by_key = (char *) group_by_result_map_iterator.key;
   //    group_by_t *group_by_result = (group_by_t *) group_by_result_map_iterator.value;

   //    printf("%s: %d items \n", group_by_key, group_by_result->last_index);
   // }

}

/* Generic interface */
static void launch_sql_parser(struct workload *w, bench_t b) {
   return _launch_sql_parser(w, 0, w->nb_requests_per_thread, 0);
}

/* Pretty printing */
static const char *name_sql_parser(bench_t w) {
   printf("SQL PARSER BENCHMARK [failure rate = %f%%, %lu commits done, %lu max parallel transactions] [snapshot size %lu] [map calls %lu]\n", ((double)failed_trans)*100./(double)nb_commits_done, nb_commits_done, get_max_recorded_parallel_transactions(), get_snapshot_size(), 0LU);
   return "SQL PARSER";
}

static int handles_sql_parser(bench_t w) {
   failed_trans = 0; // dirty, but this function is called at init time
   nb_commits_done = 0;

   switch(w) {
      case sql_parser:
         return 1;
      default:
         return 0;
   }
}

static const char* api_name_sql_parser(void) {
   return "SQL PARSER";
}

struct workload_api SQL_PARSER = {
   .handles = handles_sql_parser,
   .launch = launch_sql_parser,
   .api_name = api_name_sql_parser,
   .name = name_sql_parser,
   .create_unique_item = create_unique_item_sql_parser,
};

/**
 * SQL PARSER 
 **/

typedef enum state
{
   stepType,
   stepSelectField,
   stepSelectFrom,
   stepSelectComma,
   stepSelectFromTable,
   stepSelectFromTableComma,
   stepWhere,
   stepWhereField,
   stepWhereOperator,
   stepWhereNot,
   stepWhereComparison,
   stepWhereIn,
   stepWhereLike,
   stepWhereBetween,
   stepWhereComparisonValue,
   stepWhereLikeValue,
   stepWhereInValue,
   stepWhereBetweenValue,
   stepWhereValueType,
   stepWhereContinue,
   stepWhereAnd,
   stepWhereOr,
   stepGroupBy,
   stepGroupByComma,
   stepOrderBy,
   stepOrderByComma,
} state_t;

void str_replace(char *target, const char *needle, const char *replacement)
{
   char buffer[1024] = { 0 };
   char *insert_point = &buffer[0];
   const char *tmp = target;
   size_t needle_len = strlen(needle);
   size_t repl_len = strlen(replacement);

   while (1) 
   {
      const char *p = strstr(tmp, needle);

      // walked past last occurrence of needle; copy remaining part
      if (p == NULL) 
      {
         strcpy(insert_point, tmp);
         break;
      }

      // copy part before needle
      memcpy(insert_point, tmp, p - tmp);
      insert_point += p - tmp;

      // copy replacement string
      memcpy(insert_point, replacement, repl_len);
      insert_point += repl_len;

      // adjust pointers, move on
      tmp = p + needle_len;
   }

   // write altered string back to target
   strcpy(target, buffer);
}

void parse_string(char *p, char *str, char *correspond_table_identifier) {
    char *c;
    char t[256];

    int i=0;
    for (c=p; *c!='\0'; c++) {
        if (*c == '_') {
            t[i] = '\0';
            strcpy(correspond_table_identifier, t);
            t[0] = 0;
            i=0;
        }
        else {
            t[i] = *c;
            i++;
        }
    }

    t[i] = '\0';
    strcpy(str, t);
}

void parse_sql(char *input_sql) {
   char delim[] = " ";
   char* ptr = strtok(input_sql, delim);

   query_t *query = (query_t *) calloc(1, sizeof(query_t));
	state_t step = stepType;

   table_name_t *current_table_name = NULL;

	list_node_t *current_field_ptr = NULL;

	condition_t *current_condition_ptr = NULL;
   condition_t *current_and_condition_ptr = NULL;
   condition_t *current_or_condition_ptr = NULL;

   comparison_condition_t *current_comparison_condition = NULL;
   like_condition_t *curent_like_condition = NULL;
   between_condition_t *current_between_condition = NULL;
   in_condition_t *current_in_condition = NULL;
   list_node_t *current_in_value_ptr = NULL;
   group_by_node_t *current_group_by_ptr = NULL;
   order_by_node_t *current_order_by_ptr = NULL;

   while (ptr != NULL)
   {
      switch (step)
      {
         case stepType: 
         {
            if (strcmp(ptr, "SELECT") == 0)
            {
               query->type = SELECT;
               step = stepSelectField;

               query->field_ptr = (list_node_t *) calloc(1, sizeof(list_node_t));
               current_field_ptr = query->field_ptr;
            }

            ptr = strtok(NULL, delim);
            break;
         }
         case stepSelectField: 
         {
            char *value = NULL;
            if (strncmp("SUM(", ptr, 4) == 0)
            {
               current_field_ptr->value_type = SUM;
               value = calloc(1, strlen(ptr) - 5);
               for (int i=4; i<strlen(ptr)-1; i++)
               {
                  value[i-4] = ptr[i];
               }
            }
            else if (strncmp("AVG(", ptr, 4) == 0) 
            {
               current_field_ptr->value_type = AVG;
               value = calloc(1, strlen(ptr) - 5);
               for (int i=4; i<strlen(ptr)-1; i++)
               {
                  value[i-4] = ptr[i];
               }
            }
            else
            {
               current_field_ptr->value_type = COLUMN_FIELD;
               value = ptr;
            }

            char *field = calloc(1, sizeof(char *));
            char *correspond_table_identifier = calloc(1, sizeof(char *));
            parse_string(value, field, correspond_table_identifier);

            current_field_ptr->val = field;
            if (strcmp(correspond_table_identifier, "") != 0)
            {
               current_field_ptr->key = correspond_table_identifier;

               hashset_t table_columns = (hashset_t) ht_get(sql_tables_columns, correspond_table_identifier);
               if (table_columns == NULL) {
                  table_columns = hashset_create();
                  ht_set(sql_tables_columns, correspond_table_identifier, table_columns);
               }
               add_str_to_set(table_columns, field);
            }
            
            ptr = strtok(NULL, delim);
            if (strcmp(ptr, ",") == 0) 
            {
               step = stepSelectComma;
            }
            else if (strcmp(ptr, "AS") == 0)
            {
               ptr = strtok(NULL, delim);
               current_field_ptr->as = ptr;
               
               if (strcmp(ptr, ",") == 0)
               {
                  step = stepSelectComma;
               }
               else 
               {
                  step = stepSelectFrom;
               }
            }
            else 
            {
               step = stepSelectFrom;
            }

            break;
         }
         case stepSelectComma: 
         {
            current_field_ptr->next = (list_node_t *) calloc(1, sizeof(list_node_t));
            current_field_ptr = current_field_ptr->next;

            step = stepSelectField;
            ptr = strtok(NULL, delim);
            break;
         }
         case stepSelectFrom: 
         {
            query->table_name_ptr = (table_name_t *) calloc(1, sizeof(table_name_t));
            current_table_name = query->table_name_ptr;
            step = stepSelectFromTable;
            ptr = strtok(NULL, delim);
            break;
         }
         case stepSelectFromTable: 
         {
            current_table_name->name = ptr;
            current_table_name->identifier[0] = ptr[0];
            ht_set(table_identifier_to_table_name, current_table_name->identifier, current_table_name->name);

            ptr = strtok(NULL, delim);

            if (ptr != NULL)
            {
               if (strcmp(ptr, "WHERE") == 0)
               {
                  step = stepWhere;
               }
               else if (strcmp(ptr, ",") == 0)
               {
                  step = stepSelectFromTableComma;
               }
            }

            break;
         }
         case stepSelectFromTableComma:
         {
            current_table_name->next_table = (table_name_t *) calloc(1, sizeof(table_name_t));
            current_table_name = current_table_name->next_table;

            ptr = strtok(NULL, delim);
            step = stepSelectFromTable;
            break;
         }
         case stepWhere: 
         {
            ptr = strtok(NULL, delim);

            if (query->condition_ptr == NULL) 
            {
               query->condition_ptr = (condition_t *) calloc(1, sizeof(condition_t));
               current_condition_ptr = query->condition_ptr;
            }

            step = stepWhereField;
            break;
         }
         case stepWhereField: 
         {
            char *field = calloc(1, sizeof(char *));
            char *correspond_table_identifier = calloc(1, sizeof(char *));
            parse_string(ptr, field, correspond_table_identifier);

            current_condition_ptr->operand1 = calloc(1, sizeof(field_operand_t));
            current_condition_ptr->operand1->name = field;

            if (strcmp(correspond_table_identifier, "") != 0) 
            {
               current_condition_ptr->operand1->table_identifier = correspond_table_identifier;

               hashset_t table_columns = (hashset_t) ht_get(sql_tables_columns, correspond_table_identifier);
               if (table_columns == NULL) {
                  table_columns = hashset_create();
                  ht_set(sql_tables_columns, correspond_table_identifier, table_columns);
               }
               add_str_to_set(table_columns, field);
            }

            ptr = strtok(NULL, delim);

            if (strcmp(ptr, "NOT") == 0) 
            {
               step = stepWhereNot;
            }
            else 
            {
               step = stepWhereOperator;
            }

            break;
         }
         case stepWhereNot: 
         {
            current_condition_ptr->not = true;
            ptr = strtok(NULL, delim);
            step = stepWhereOperator;
            break;
         }
         case stepWhereOperator: 
         {
            if (strcmp(ptr, "LIKE") == 0) 
            {
               current_condition_ptr->operator = LIKE;
               step = stepWhereLike;
            }
            else if (strcmp(ptr, "IN") == 0) {
               current_condition_ptr->operator = IN;
               step = stepWhereIn;
            }
            else if (strcmp(ptr, "BETWEEN") == 0) 
            {
               current_condition_ptr->operator = BETWEEN;
               step = stepWhereBetween;
            }
            else if (strcmp(ptr, "=") == 0) 
            {
               current_condition_ptr->operator = EQ;
               step = stepWhereComparison;
            }
            else if (strcmp(ptr, ">") == 0)
            {
               current_condition_ptr->operator = GT;
               step = stepWhereComparison;
            }
            else if (strcmp(ptr, "<") == 0)
            {
               current_condition_ptr->operator = LT;
               step = stepWhereComparison;
            }
            else if (strcmp(ptr, ">=") == 0)
            {
               current_condition_ptr->operator = GTE;
               step = stepWhereComparison;
            }
            else if (strcmp(ptr, "<=") == 0)
            {
               current_condition_ptr->operator = LTE;
               step = stepWhereComparison;
            }
            else if (strcmp(ptr, "!=") == 0)
            {
               current_condition_ptr->operator = NE;
               step = stepWhereComparison;
            }

            ptr = strtok(NULL, delim);
            break;
         }
         case stepWhereComparison:
         {
            current_comparison_condition = (comparison_condition_t *) calloc(1, sizeof(comparison_condition_t));

            current_condition_ptr->operand2 = current_comparison_condition;
            step = stepWhereComparisonValue;
            break;
         }
         case stepWhereLike: 
         {
            curent_like_condition = (like_condition_t *) calloc(1, sizeof(like_condition_t));
            curent_like_condition->ex = ptr;

            current_condition_ptr->operand2 = curent_like_condition;
            step = stepWhereLikeValue;
            break;
         }
         case stepWhereBetween:
         {
            current_between_condition = (between_condition_t *) calloc(1, sizeof(between_condition_t));

            current_condition_ptr->operand2 = current_between_condition;
            step = stepWhereBetweenValue;
            break;
         }
         case stepWhereIn: 
         {
            current_in_condition = (in_condition_t *) calloc(1, sizeof(in_condition_t));
            current_in_condition->match_ptr = (list_node_t *) calloc(1, sizeof(list_node_t));
            current_in_value_ptr = current_in_condition->match_ptr;

            current_condition_ptr->operand2 = current_in_condition;
            step = stepWhereInValue;
            break;
         }
         case stepWhereComparisonValue:
         {
            char *value = calloc(1, sizeof(char *));
            char *correspond_table_identifier = calloc(1, sizeof(char *));
            parse_string(ptr, value, correspond_table_identifier);

            current_comparison_condition->value = value;
            if (strcmp(correspond_table_identifier, "") != 0)
            {
               current_condition_ptr->is_join_condition = true;
               current_comparison_condition->value_type = COLUMN_FIELD;
               current_comparison_condition->table = correspond_table_identifier;
               hashset_t table_columns = (hashset_t) ht_get(sql_tables_columns, correspond_table_identifier);
               if (table_columns == NULL) {
                  table_columns = hashset_create();
                  ht_set(sql_tables_columns, correspond_table_identifier, table_columns);
               }
               add_str_to_set(table_columns, value);
            }
            else 
            {
               current_comparison_condition->value_type = CONSTANT;
            }

            ptr = strtok(NULL, delim);
            step = stepWhereValueType;
            break;
         }
         case stepWhereLikeValue: 
         {
            char *required_ex = calloc(1, strlen(ptr));
            strcpy(required_ex, ptr);
            required_ex[0] = '^';
            required_ex[strlen(required_ex) - 1] = '$';

            str_replace(required_ex, "%", ".*");

            regcomp(&(curent_like_condition->regex), required_ex, 0);

            ptr = strtok(NULL, delim);
            step = stepWhereContinue;

            break;
         }
         case stepWhereBetweenValue: 
         {
            char *between_value = calloc(1, strlen(ptr));
            strcpy(between_value, ptr);

            // remove '()'
            if (between_value[0] == '(') 
            {
               between_value += 1;
            }
            if (between_value[strlen(between_value) - 1] == ')') 
            {
               between_value[strlen(between_value) - 1] = '\0';
            }

            // remove ''
            if (between_value[0] == '\'') 
            {
               between_value += 1;
            }
            if (between_value[strlen(between_value) - 1] == '\'') 
            {
               between_value[strlen(between_value) - 1] = '\0';
            }

            if (current_between_condition->min_value == NULL) 
            {
               current_between_condition->min_value = ptr;
               current_between_condition->min_value_type = CONSTANT;
            }
            else if (current_between_condition->max_value == NULL) {
               current_between_condition->max_value = ptr;
               current_between_condition->max_value_type = CONSTANT;
            }

            ptr = strtok(NULL, delim);
            if (ptr != NULL) {
               if (strcmp(ptr, "AND") == 0) {
                  ptr = strtok(NULL, delim);
                  step = stepWhereBetweenValue;
               }
               else {
                  step = stepWhereValueType;
               }
            }
            break;
         }
         case stepWhereInValue: 
         {
            char *in_value = calloc(1, strlen(ptr));
            strcpy(in_value, ptr);

            // remove '()'
            if (in_value[0] == '(') 
            {
               in_value += 1;
            }
            if (in_value[strlen(in_value) - 1] == ')') 
            {
               in_value[strlen(in_value) - 1] = '\0';
            }

            // remove ''
            if (in_value[0] == '\'') 
            {
               in_value += 1;
            }
            if (in_value[strlen(in_value) - 1] == '\'') 
            {
               in_value[strlen(in_value) - 1] = '\0';
            }

            current_in_value_ptr->val = in_value;

            ptr = strtok(NULL, delim);
            if (ptr != NULL && strcmp(ptr, ",") == 0) 
            {
               current_in_value_ptr->next = (list_node_t *) calloc(1, sizeof(list_node_t));
               current_in_value_ptr = current_in_value_ptr->next;

               ptr = strtok(NULL, delim);
               step = stepWhereInValue;
            }
            else 
            {
               step = stepWhereContinue;
            }

            break;
         }
         case stepWhereValueType:
         {
            if (ptr != NULL) {
               if (strcmp(ptr, "+") == 0 || strcmp(ptr, "-") == 0 || strcmp(ptr, "*") == 0 || strcmp(ptr, "/") == 0) 
               {
                  arithmetic_condition_t *athm_condition = (arithmetic_condition_t *) calloc(1, sizeof(arithmetic_condition_t));
                  athm_condition->operator = ptr;
                  ptr = strtok(NULL, delim);
                  athm_condition->operand2 = ptr;
                  switch (current_condition_ptr->operator)
                  {
                     case EQ:
                     case GT:
                     case LT:
                     case GTE:
                     case LTE:
                     case NE:
                     {
                        athm_condition->operand1 = (char *) current_comparison_condition->value;
                        
                        current_comparison_condition->value = athm_condition;
                        current_comparison_condition->value_type = ARITHMETIC;

                        ptr = strtok(NULL, delim);
                        step = stepWhereContinue;
                        break;
                     }
                     case BETWEEN:
                     {
                        if (current_between_condition->max_value == NULL) {
                           athm_condition->operand1 = current_between_condition->min_value;

                           current_between_condition->min_value = athm_condition;
                           current_between_condition->min_value_type = ARITHMETIC;

                           ptr = strtok(NULL, delim);
                           if (strcmp(ptr, "AND") == 0) {
                              ptr = strtok(NULL, delim);
                              step = stepWhereBetweenValue;
                           }
                        }
                        else 
                        {
                           athm_condition->operand1 = current_between_condition->max_value;

                           current_between_condition->max_value = athm_condition;
                           current_between_condition->max_value_type = ARITHMETIC;

                           ptr = strtok(NULL, delim);
                           step = stepWhereContinue;
                        }
                        break;
                     }
                     default:
                     {
                        break;
                     }
                  }
               }
               else
               {
                  step = stepWhereContinue;
               }
            } 
            break;
         }
         case stepWhereContinue: 
         {
            if (ptr != NULL) {
               if (strcmp(ptr, "AND") == 0) 
               {
                  step = stepWhereAnd;
               }
               else if (strcmp(ptr, "OR") == 0)
               {
                  step = stepWhereOr;
               }
               else if (strcmp(ptr, "GROUP") == 0)
               {
                  ptr = strtok(NULL, delim);
                  if (strcmp(ptr, "BY") == 0) 
                  {
                     ptr = strtok(NULL, delim);
                     query->group_by_ptr = (group_by_node_t *) calloc(1, sizeof(group_by_node_t));
                     current_group_by_ptr = query->group_by_ptr;
                     step = stepGroupBy;
                  }
               }
               else if (strcmp(ptr, "ORDER") == 0)
               {
                  ptr = strtok(NULL, delim);
                  if (strcmp(ptr, "BY") == 0) 
                  {
                     ptr = strtok(NULL, delim);
                     query->order_by_ptr = (order_by_node_t *) calloc(1, sizeof(order_by_node_t));
                     current_order_by_ptr = query->group_by_ptr;
                     step = stepOrderBy;
                  }
               }
               break;
            }
            break;
         }
         case stepWhereAnd: 
         {
            if (current_and_condition_ptr == NULL)
            {
               query->and_condition_ptr = (condition_t *) calloc(1, sizeof(condition_t));
               current_and_condition_ptr = query->and_condition_ptr;
            }
            else 
            {
               current_and_condition_ptr->next_condition = (condition_t *) calloc(1, sizeof(condition_t));
               current_and_condition_ptr = current_and_condition_ptr->next_condition;
            }

            current_condition_ptr = current_and_condition_ptr;
            ptr = strtok(NULL, delim);
            step = stepWhereField;
            break;
         }
         case stepWhereOr: 
         {
            if (query->or_condition_ptr == NULL) 
            {
               query->or_condition_ptr = (condition_t *) calloc(1, sizeof(condition_t));
               current_or_condition_ptr = query->or_condition_ptr;
            }
            else
            {
               current_or_condition_ptr->next_condition = (condition_t *) calloc(1, sizeof(condition_t));
               current_or_condition_ptr = current_or_condition_ptr->next_condition;
            }

            current_condition_ptr = current_or_condition_ptr;
            ptr = strtok(NULL, delim);
            step = stepWhereField;
            break;
         }
         case stepGroupBy:
         {
            char *field = calloc(1, sizeof(char *));
            char *correspond_table_identifier = calloc(1, sizeof(char *));
            parse_string(ptr, field, correspond_table_identifier);

            current_group_by_ptr->val = field;
            if (strcmp(correspond_table_identifier, "") != 0)
            {
               current_group_by_ptr->key = correspond_table_identifier;
            }

            ptr = strtok(NULL, delim);
            if (ptr != NULL)
            {
               if (strcmp(ptr, ",") == 0)
               {
                  step = stepGroupByComma;
               }
               else if (strcmp(ptr, "ORDER") == 0)
               {
                  ptr = strtok(NULL, delim);
                  if (strcmp(ptr, "BY") == 0)
                  {
                     ptr = strtok(NULL, delim);
                     query->order_by_ptr = (order_by_node_t *) calloc(1, sizeof(order_by_node_t));
                     current_order_by_ptr = query->order_by_ptr;
                     step = stepOrderBy;
                  }
               }
            }
            break;
         }
         case stepGroupByComma:
         {
            current_group_by_ptr->next = (group_by_node_t *) calloc(1, sizeof(group_by_node_t));
            current_group_by_ptr = current_group_by_ptr->next;
            
            ptr = strtok(NULL, delim);
            step = stepGroupBy;
            break;
         }
         case stepOrderBy:
         {
            current_order_by_ptr->desc = false;
            char *field = calloc(1, sizeof(char *));
            char *correspond_table_identifier = calloc(1, sizeof(char *));
            parse_string(ptr, field, correspond_table_identifier);
            
            current_order_by_ptr->val = field;
            if (strcmp(correspond_table_identifier, "") != 0)
            {
               current_order_by_ptr->key = correspond_table_identifier;
            }

            ptr = strtok(NULL, delim);
            if (ptr != NULL) 
            {
               if (strcmp(ptr, ",") == 0)
               {
                  step = stepOrderByComma;
               }
               else if (strcmp(ptr, "DESC") == 0)
               {
                  current_order_by_ptr->desc = true;
                  ptr = strtok(NULL, delim);
                  if (ptr != NULL)
                  {
                     if (strcmp(ptr, ",") == 0)
                     {
                        step = stepOrderByComma;
                     }
                  }
               }
            }
            break;
         }
         case stepOrderByComma:
         {
            current_order_by_ptr->next = (order_by_node_t *) calloc(1, sizeof(order_by_node_t));
            current_order_by_ptr = current_order_by_ptr->next;
            
            ptr = strtok(NULL, delim);
            step = stepOrderBy;
            break;
         }
         default: 
         {
            ptr = strtok(NULL, delim);
            break;
         }
      }
   }

   origin_query = query;
   printf("\nOrigin Query object\n");
   print_query_object(origin_query);
   printf("\n");

   sub_queries_map = ht_create();
   query_t *current_sub_query;

   hti sql_tables_columns_iterator = ht_iterator(sql_tables_columns);
   while (ht_next(&sql_tables_columns_iterator)) {
      char *selected_table_identifier = (char *) sql_tables_columns_iterator.key;
      hashset_t table_columns = (hashset_t) sql_tables_columns_iterator.value;
      hashset_itr_t table_columns_iter = hashset_iterator(table_columns);

      current_sub_query = (query_t *) calloc(1, sizeof(query_t));
      current_sub_query->type = SELECT;
      current_sub_query->table_name_ptr = (table_name_t *) calloc(1, sizeof(table_name_t));
      current_sub_query->table_name_ptr->name = ht_get(table_identifier_to_table_name, selected_table_identifier);
      strcpy(current_sub_query->table_name_ptr->identifier, selected_table_identifier);
      
      current_sub_query->field_ptr = calloc(1, sizeof(list_node_t));
      list_node_t *current_field_ptr = current_sub_query->field_ptr;

      while(hashset_iterator_has_next(table_columns_iter))
      {
         current_field_ptr->val = (char *) hashset_iterator_value(table_columns_iter);
         current_field_ptr->key = (char *) sql_tables_columns_iterator.key;
         current_field_ptr->value_type = COLUMN_FIELD;

         // printf("%s ", current_field_ptr->val);
         hashset_iterator_next(table_columns_iter);
         if (hashset_iterator_has_next(table_columns_iter))
         {
            current_field_ptr->next = calloc(1, sizeof(list_node_t));
            current_field_ptr = current_field_ptr->next;
         }
      }
      
      // add conditions too
      if (origin_query->condition_ptr != NULL)
      {
         condition_t *origin_condition_ptr = origin_query->condition_ptr;
         if (strcmp(origin_condition_ptr->operand1->table_identifier, selected_table_identifier) == 0 && !origin_condition_ptr->is_join_condition)
         {
            condition_t *new_condition_ptr = (condition_t *) calloc(1, sizeof(condition_t));
            new_condition_ptr->operand1 = origin_condition_ptr->operand1;
            new_condition_ptr->operator = origin_condition_ptr->operator;
            new_condition_ptr->operand2 = origin_condition_ptr->operand2;
            new_condition_ptr->not = origin_condition_ptr->not;

            current_sub_query->condition_ptr = new_condition_ptr;
         }
      }

      condition_t *origin_and_condition_ptr = origin_query->and_condition_ptr;
      condition_t *current_sub_query_and_condition_ptr;
      while (origin_and_condition_ptr != NULL)
      {
         if (strcmp(origin_and_condition_ptr->operand1->table_identifier, selected_table_identifier) == 0 && !origin_and_condition_ptr->is_join_condition)
         {
            if (current_sub_query->and_condition_ptr == NULL)
            {
               current_sub_query->and_condition_ptr = (condition_t *) calloc(1, sizeof(condition_t));
               current_sub_query_and_condition_ptr = current_sub_query->and_condition_ptr;
            }
            else {
               current_sub_query_and_condition_ptr->next_condition = (condition_t *) calloc(1, sizeof(condition_t));
               current_sub_query_and_condition_ptr = current_sub_query_and_condition_ptr->next_condition;
            }

            current_sub_query_and_condition_ptr->operand1 = origin_and_condition_ptr->operand1;
            current_sub_query_and_condition_ptr->operator = origin_and_condition_ptr->operator;
            current_sub_query_and_condition_ptr->operand2 = origin_and_condition_ptr->operand2;
            current_sub_query_and_condition_ptr->not = origin_and_condition_ptr->not;
         }
         origin_and_condition_ptr = origin_and_condition_ptr->next_condition;
      }

      condition_t *origin_or_condition_ptr = origin_query->or_condition_ptr;
      condition_t *current_sub_query_or_condition_ptr;
      while (origin_or_condition_ptr != NULL)
      {
         if (strcmp(origin_or_condition_ptr->operand1->table_identifier, selected_table_identifier) == 0 && !origin_or_condition_ptr->is_join_condition)
         {
            if (current_sub_query->or_condition_ptr == NULL)
            {
               current_sub_query->or_condition_ptr = (condition_t *) calloc(1, sizeof(condition_t));
               current_sub_query_or_condition_ptr = current_sub_query->or_condition_ptr;
            }
            else {
               current_sub_query_or_condition_ptr->next_condition = (condition_t *) calloc(1, sizeof(condition_t));
               current_sub_query_or_condition_ptr = current_sub_query_or_condition_ptr->next_condition;
            }

            current_sub_query_or_condition_ptr->operand1 = origin_or_condition_ptr->operand1;
            current_sub_query_or_condition_ptr->operator = origin_or_condition_ptr->operator;
            current_sub_query_or_condition_ptr->operand2 = origin_or_condition_ptr->operand2;
            current_sub_query_or_condition_ptr->not = origin_or_condition_ptr->not;
         }
         origin_or_condition_ptr = origin_or_condition_ptr->next_condition;
      }

      // printf("------------------------------------\n");
      // print_query_object(current_sub_query);
      ht_set(sub_queries_map, selected_table_identifier, current_sub_query);
   }

   return;
}

char* check_operator(operator_t op) 
{
   switch (op)
   {
   case EQ:
      return "=";
   case GT:
      return ">";
   case LT:
      return "<";
   case GTE:
      return ">=";
   case LTE:
      return "<=";
   case NE:
      return "!=";
   case LIKE:
      return "LIKE";
   case BETWEEN:
      return "BETWEEN";
   case IN:
      return "IN";
   default:
      break;
   }

   return NULL;
}

void print_where_condition(condition_t *cond) 
{
   char *operator_to_print = check_operator(cond->operator);
   char *operand_print = calloc(1, sizeof(char *));
   if (strcmp(operator_to_print, "LIKE") == 0) 
   {
      strcpy(operand_print, ((like_condition_t *) cond->operand2)->ex);
   }
   else if (strcmp(operator_to_print, "BETWEEN") == 0)
   {
      strcpy(operand_print, "(");
      between_condition_t *between_condition = (between_condition_t *) cond->operand2;
      switch (between_condition->min_value_type)
      {
         case CONSTANT:
         {
            strcat(operand_print, (char *)(between_condition->min_value));
            break;
         }
         case ARITHMETIC:
         {
            arithmetic_condition_t *athm_cond = (arithmetic_condition_t *)(between_condition->min_value);
            strcat(operand_print, athm_cond->operand1);
            strcat(operand_print, " ");
            strcat(operand_print, athm_cond->operator);
            strcat(operand_print, " ");
            strcat(operand_print, athm_cond->operand2);
            break;
         }
         default:
         {
            break;
         }
      }
      strcat(operand_print, " AND ");
      switch (between_condition->max_value_type)
      {
         case CONSTANT:
         {
            strcat(operand_print, (char *)(between_condition->max_value));
            break;
         }
         case ARITHMETIC:
         {
            arithmetic_condition_t *athm_cond = (arithmetic_condition_t *)(between_condition->max_value);
            strcat(operand_print, athm_cond->operand1);
            strcat(operand_print, " ");
            strcat(operand_print, athm_cond->operator);
            strcat(operand_print, " ");
            strcat(operand_print, athm_cond->operand2);
            break;
         }
         default:
         {
            break;
         }
      }
      strcat(operand_print, ")");
   }
   else if (strcmp(operator_to_print, "IN") == 0) 
   {
      strcpy(operand_print, "(");
      list_node_t *cur_node = ((in_condition_t *) cond->operand2)->match_ptr;
      while (cur_node != NULL)
      {
         strcat(operand_print, cur_node->val);
         cur_node = cur_node->next;

         if (cur_node != NULL) 
         {
            strcat(operand_print, ", ");
         }
      }
      strcat(operand_print, ")");
   }
   else {
      comparison_condition_t *comparison_condition = (comparison_condition_t *) cond->operand2;
      switch (comparison_condition->value_type)
      {
         case CONSTANT:
         {
            strcpy(operand_print, (char *)(comparison_condition->value));
            break;
         }
         case COLUMN_FIELD:
         {
            strcpy(operand_print, comparison_condition->table);
            strcat(operand_print, "_");
            strcat(operand_print, comparison_condition->value);
            break;
         }
         case ARITHMETIC: 
         {
            arithmetic_condition_t *athm_cond = (arithmetic_condition_t *)(comparison_condition->value);
            strcpy(operand_print, athm_cond->operand1);
            strcat(operand_print, " ");
            strcat(operand_print, athm_cond->operator);
            strcat(operand_print, " ");
            strcat(operand_print, athm_cond->operand2);
            break;
         }
         default:
         {
            break;
         }
      }
   }

   char *not = cond->not ? "NOT" : "\b";
   char *table = cond->operand1->table_identifier == NULL ? "" : cond->operand1->table_identifier;
   char table_delim = cond->operand1->table_identifier == NULL ? '\0' : '_';
   
   printf("\"%s%c%s %s %s %s\", ", table, table_delim, cond->operand1->name, not, operator_to_print, operand_print);
}


void print_query_object(query_t *query) 
{
   if (query->type == SELECT) 
   {
      printf("The query type is \"SELECT\" \n");
   }

   list_node_t *current_field = query->field_ptr;
   printf("The select fields are ");
   while (current_field != NULL) 
   {  
      char *table = current_field->key == NULL ? "" : current_field->key;
      char table_delim = current_field->key == NULL ? '\0' : '_';
      
      char *field_value;

      switch (current_field->value_type)
      {
      case SUM:
         asprintf(&field_value, "SUM(\"%s%c%s\")", table, table_delim, current_field->val);
         break;
      case AVG:
         asprintf(&field_value, "AVG(\"%s%c%s\")", table, table_delim, current_field->val);
         break;
      case COLUMN_FIELD:
         asprintf(&field_value, "\"%s%c%s\"", table, table_delim, current_field->val);
         break;
      default:
         break;
      }
      
      if (current_field->as != NULL)
      {
         asprintf(&field_value, "%s AS %s", field_value, current_field->as);
      }

      printf("%s, ", field_value);
      current_field = current_field->next;
   }
   printf("\n");

   table_name_t *current_table_name = query->table_name_ptr;
   printf("The tables are ");
   while (current_table_name != NULL) {
      printf("\"%s (%s)\", ", current_table_name->name, current_table_name->identifier);
      current_table_name = current_table_name->next_table;
   }
   printf("\n");

   condition_t *current_condition = query->condition_ptr;
   printf("The condition is ");

   if (current_condition != NULL) 
   {
      print_where_condition(current_condition);
   }
   printf("\n");

   current_condition = query->and_condition_ptr;
   printf("The AND conditions are ");
   while (current_condition != NULL) 
   {   
      print_where_condition(current_condition);
      current_condition = current_condition->next_condition;
   }
   printf("\n");

   current_condition = query->or_condition_ptr;
   printf("The OR conditions are ");
   while (current_condition != NULL) 
   {
      print_where_condition(current_condition);
      current_condition = current_condition->next_condition;
   }
   printf("\n");

   printf("The GROUP BY columns are ");
   group_by_node_t *current_group_by_ptr = query->group_by_ptr;
   while (current_group_by_ptr != NULL)
   {
      char *table = current_group_by_ptr->key == NULL ? "" : current_group_by_ptr->key;
      char table_delim = current_group_by_ptr->key == NULL ? '\0' : '_';
      printf("\"%s%c%s\", ", table, table_delim, current_group_by_ptr->val);
      current_group_by_ptr = current_group_by_ptr->next;

   }
   printf("\n");

   printf("The ORDER BY columns are ");
   order_by_node_t *current_order_by_ptr = query->order_by_ptr;
   while (current_order_by_ptr != NULL)
   {
      char *table = current_order_by_ptr->key == NULL ? "" : current_order_by_ptr->key;
      char table_delim = current_order_by_ptr->key == NULL ? '\0' : '_';
      if (current_order_by_ptr->desc)
      {
         printf("\"%s%c%s DESC\", ", table, table_delim, current_order_by_ptr->val);
      }
      else
      {
         printf("\"%s%c%s\", ", table, table_delim, current_order_by_ptr->val);
      }
      
      current_order_by_ptr = current_order_by_ptr->next;
   }
   printf("\n");
}