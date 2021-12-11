/*
 * SQL PARSER workload
 */

#include "headers.h"
#include "workload-common.h"
#include "workload-sql-parser.h"
#include "hashtable.h"
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
enum table { LINEITEM, PART, PARTSUPP, ORDERS, SUPPLIERS, CUSTOMER, NATION, REGION };

enum column {
   ORDERKEY, PARTKEY, SUPPKEY, LINENUMBER, QUANTITY, EXTENDEDPRICE, DISCOUNT, TAX, RETURNFLAG, LINESTATUS, SHIPDATE, COMMITDATE, RECEIPTDATE, SHIPINSTRUCT, SHIPMODE, COMMENT,
};

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

char *add_column_value(char *item, char *column_name, void *value) {
   struct column_info *ci = ht_get(test_table->column_map, column_name);
   switch (ci->type)
   {
   case INT:
      return add_shash_uint(item, *((int *) ht_get(test_table->column_map, column_name)), value, 1);
      break;
   case STRING:
      return add_shash_specific_string(item, *((int *) ht_get(test_table->column_map, column_name)), value, 1);
      break;
   default:
      break;
   }
}

static char* get_column_string_value(char* item, char* column_name) {
   struct column_info *ci = ht_get(test_table->column_map, column_name);
   
   char *tmp = malloc(256);
   switch (ci->type)
   {
   case INT:
      sprintf(tmp, "%ld", get_shash_uint(item, ci->index));
      break;
   case STRING:
      sprintf(tmp, "%s", get_shash_string(item, ci->index));
      break;
   default:
      break;
   }

   return tmp;
}

bool compare_column_value(char *item, char *column_name, operator_t operator, void *expected_value, bool not) {
   struct column_info *ci = ht_get(test_table->column_map, column_name);
   void *item_value = get_column_string_value(item, column_name);

   bool res;
   switch (operator)
   {
      case EQ: 
      {
         switch (ci->type)
         {
            case INT:
               res = get_shash_uint(item, ci->index) == atoi(expected_value);
               break;
            case STRING:
               res = strcmp(get_shash_string(item, ci->index), (char *) expected_value) == 0;
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
               res = get_shash_uint(item, ci->index) > atoi(expected_value);
               break;
            case STRING:
               res = strcmp(get_shash_string(item, ci->index), (char *) expected_value) > 0;
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
               res = get_shash_uint(item, ci->index) < atoi(expected_value);
               break;
            case STRING:
               res = strcmp(get_shash_string(item, ci->index), (char *) expected_value) < 0;
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
               res = get_shash_uint(item, ci->index) >= atoi(expected_value);
               break;
            case STRING:
               res = strcmp(get_shash_string(item, ci->index), (char *) expected_value) >= 0;
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
               res = get_shash_uint(item, ci->index) <= atoi(expected_value);
               break;
            case STRING:
               res = strcmp(get_shash_string(item, ci->index), (char *) expected_value) <= 0;
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
               res = get_shash_uint(item, ci->index) != atoi(expected_value);
               break;
            case STRING:
               res = strcmp(get_shash_string(item, ci->index), (char *) expected_value) != 0;
               break;
            default:
               break;
         }
         break;
      }
      case LIKE:
      {
         int reti = regexec(&(((like_condition_t *) expected_value)->regex), get_shash_string(item, ci->index), 0, NULL, 0);
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
         between_condition_t *between_condition = (between_condition_t *) expected_value;
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
         list_node_t *cur_node = ((in_condition_t *) expected_value)->match_ptr;
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
   char *result;
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

bool evaluate_where_condition(char *item, char *column_name, condition_t *condition) {
   struct column_info *ci = ht_get(test_table->column_map, column_name);
   switch (condition->operator)
   {
      case EQ:
      case GT:
      case LT:
      case GTE:
      case LTE:
      case NE:
      {
         simple_condition_t *simple_condition = (simple_condition_t *) condition->operand2;
         switch (simple_condition->value_type)
         {
            case SINGLE:
            {
               return compare_column_value(item, column_name, condition->operator, (char *) simple_condition->value, condition->not);
               break;
            }
            case ARITHMETIC:
            {
               arithmetic_condition_t *arithmetic_condition = (arithmetic_condition_t *) simple_condition->value;
               char *expected_value = perform_arithmetic(arithmetic_condition);
               return compare_column_value(item, column_name, condition->operator, expected_value, condition->not);
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
         between_condition_t *new_between_condition = (between_condition_t *) malloc(sizeof(between_condition_t));
         new_between_condition->min_value_type = SINGLE;
         new_between_condition->max_value_type = SINGLE;

         switch (old_between_condition->min_value_type)
         {
            case SINGLE:
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
            case SINGLE:
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

         return compare_column_value(item, column_name, condition->operator, new_between_condition, condition->not);
      }
      default:
      {
         return compare_column_value(item, column_name, condition->operator, condition->operand2, condition->not);
         break;
      }
   }
}

/*
 * SQL PARSER loader
 * Generate all the elements that will be stored in the DB
 */

static char *sql_parser_lineitem(uint64_t uid) {
   uint64_t key = sql_parser_get_key_lineitem(uid);
   char *item = create_shash(key);
   return item;
}

/*
 * Function called by the workload-common interface. uid goes from 0 to the number of elements in the DB.
 */
static char* create_unique_item_sql_parser(uint64_t uid, uint64_t max_uid) {
   if(uid == 0)
      assert(max_uid == get_db_size_sql_parser()); // set .nb_items_in_db = get_db_size_sql_parser() in main.c otherwise the DB will be partially populated

   if(uid < NB_LINEITEMS) 
   {
      char *item = sql_parser_lineitem(uid);

      item = add_column_value(item, "LINENUMBER", uid);
      item = add_column_value(item, "QUANTITY", uid * 10);
      item = add_column_value(item, "DISCOUNT", rand_between(100, 999));
      item = add_column_value(item, "TAX", rand_between(100, 999));

      if (uid % 3 == 0) {
         item = add_column_value(item, "SHIPDATE", "1998-09-03");
         item = add_column_value(item, "RETURNFLAG", "A");
         item = add_column_value(item, "LINESTATUS", "1");
      }
      else if (uid % 3 == 1) {
         item = add_column_value(item, "SHIPDATE", "1998-09-04");
         item = add_column_value(item, "RETURNFLAG", "B");
         item = add_column_value(item, "LINESTATUS", "2");
      }
      else {
         item = add_column_value(item, "SHIPDATE", "1998-11-05");
         item = add_column_value(item, "RETURNFLAG", "C");
         item = add_column_value(item, "LINESTATUS", "3");
      }

      return item;
      // return sql_parser_lineitem(uid);
   }
   uid -= NB_LINEITEMS;

   return NULL;
}


size_t get_db_size_sql_parser(void) {
   return NB_LINEITEMS
      ;
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
      long item_uid = get_shash_uint(item, *((int *) ht_get(test_table->column_map, "LINENUMBER")));
      long item_quantity = get_shash_uint(item, *((int *) ht_get(test_table->column_map, "QUANTITY")));
      char* item_returnflag = get_shash_string(item, *((int *) ht_get(test_table->column_map, "RETURNFLAG")));
      char* item_linestatus = get_shash_string(item, *((int *) ht_get(test_table->column_map, "LINESTATUS")));
      char* item_shipdate = get_shash_string(item, *((int *) ht_get(test_table->column_map, "SHIPDATE")));

      if (strcmp(item_shipdate, "1998-09-04") < 0 || strcmp(item_shipdate, "1998-09-04") == 0) {         
         char *key;
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
      bool valid_conds = true;
      // check if item respect the where condition
      condition_t *current_condition = query->condition_ptr;
      if (current_condition != NULL) {
         bool b = evaluate_where_condition(item, current_condition->operand1, current_condition);
         valid_conds = valid_conds && b;
      }

      current_condition = query->and_condition_ptr;
      while (current_condition != NULL)
      {
         bool b = evaluate_where_condition(item, current_condition->operand1, current_condition);
         valid_conds = valid_conds && b;
         if (!valid_conds) 
         {
            break;
         }
         current_condition = current_condition->next_condition;
      }

      current_condition = query->or_condition_ptr;
      while (current_condition != NULL)
      {
         bool b = evaluate_where_condition(item, current_condition->operand1, current_condition);
         valid_conds = valid_conds || b;

         if (valid_conds) {
            break;
         }
         
         current_condition = current_condition->next_condition;
      }

      if (!valid_conds) {
         return;
      }

      // item pass the conditions check
      char *key_string = get_column_string_value(item, "LINENUMBER");

      list_node_t* current_field = query->field_ptr;
      char *value = malloc(1024);
      while (current_field != NULL)
      {
         strcat(value, current_field->val);
         strcat(value, ": ");
         strcat(value, get_column_string_value(item, current_field->val));
         strcat(value, ", ");
         
         current_field = current_field->next;
      }
      // printf("%s", value);
      // printf("\n");

      ht_set(p->map, key_string, value);
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

      hti iterator = ht_iterator(returned_payload->map);
      while (ht_next(&iterator)) {
         printf("%s -> %s \n", iterator.key, iterator.value);
      }

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

static int injector_uid;

static void _launch_sql_parser(struct workload *w, int test, int nb_requests, int zipfian) {
   struct injector_queue *q = create_new_injector_queue();
   int uid = __sync_fetch_and_add(&injector_uid, 1);

   struct injector_bg_work _p = { .uid = uid };
   struct_start_timer(&_p);

   declare_periodic_count;
   size_t nb_queries = 0;

   _p.scan_cb = start_background_scan(uid, w, q, sql_parser_get_key_lineitem(0), sql_parser_get_key_lineitem(NB_LINEITEMS));

   do {
      // periodic_count(1000, "TRANS Load Injector %d [failure rate = %lu%%] [snapshot size %lu] [running transactions %lu] [%lu scans done - %d full] [%lu queries done]", uid, failed_trans*100/nb_commits_done, get_snapshot_size(), get_nb_running_transactions(), scan_progress(_p.scan_cb), glob_nb_scans_done, nb_queries);
      do_injector_bg_work(w, q, &_p);
   } while(pending_work() || injector_has_pending_work(q));
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
   stepWhere,
   stepWhereField,
   stepWhereOperator,
   stepWhereNot,
   stepWhereEquality,
   stepWhereIn,
   stepWhereLike,
   stepWhereBetween,
   stepWhereEqualityValue,
   stepWhereLikeValue,
   stepWhereInValue,
   stepWhereBetweenValue,
   stepWhereValueType,
   stepWhereContinue,
   stepWhereAnd,
   stepWhereOr,
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

query_t* parse_sql(char *input_sql) {
   char delim[] = " ";
   char* ptr = strtok(input_sql, delim);

	query_t *query = (query_t *) calloc(1, sizeof(query_t));
	state_t step = stepType;
	list_node_t *current_field_ptr = NULL;

	condition_t *current_condition_ptr = NULL;
   condition_t *current_and_condition_ptr = NULL;
   condition_t *current_or_condition_ptr = NULL;

   simple_condition_t *current_euqality_condition = NULL;
   like_condition_t *curent_like_condition = NULL;
   between_condition_t *current_between_condition = NULL;
   in_condition_t *current_in_condition = NULL;
   list_node_t *current_in_value_ptr = NULL;

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
            current_field_ptr->val = ptr;
            step = stepSelectFrom;

            ptr = strtok(NULL, delim);
            if (strcmp(ptr, ",") == 0) 
            {
               step = stepSelectComma;
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
            step = stepSelectFromTable;
            ptr = strtok(NULL, delim);
            break;
         }
         case stepSelectFromTable: 
         {
            query->table_name = ptr;
            ptr = strtok(NULL, delim);

            if (ptr != NULL && strcmp(ptr, "WHERE") == 0)
            {
               step = stepWhere;
            }

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
            current_condition_ptr->operand1 = ptr;
            current_condition_ptr->not = false;

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
               step = stepWhereEquality;
            }
            else if (strcmp(ptr, ">") == 0)
            {
               current_condition_ptr->operator = GT;
               step = stepWhereEquality;
            }
            else if (strcmp(ptr, "<") == 0)
            {
               current_condition_ptr->operator = LT;
               step = stepWhereEquality;
            }
            else if (strcmp(ptr, ">=") == 0)
            {
               current_condition_ptr->operator = GTE;
               step = stepWhereEquality;
            }
            else if (strcmp(ptr, "<=") == 0)
            {
               current_condition_ptr->operator = LTE;
               step = stepWhereEquality;
            }
            else if (strcmp(ptr, "!=") == 0)
            {
               current_condition_ptr->operator = NE;
               step = stepWhereEquality;
            }

            ptr = strtok(NULL, delim);
            break;
         }
         case stepWhereEquality:
         {
            current_euqality_condition = (simple_condition_t *) malloc(sizeof(simple_condition_t));

            current_condition_ptr->operand2 = current_euqality_condition;
            step = stepWhereEqualityValue;
            break;
         }
         case stepWhereLike: 
         {
            curent_like_condition = (like_condition_t *) malloc(sizeof(like_condition_t));
            curent_like_condition->ex = ptr;

            current_condition_ptr->operand2 = curent_like_condition;
            step = stepWhereLikeValue;
            break;
         }
         case stepWhereBetween:
         {
            current_between_condition = (between_condition_t *) malloc(sizeof(between_condition_t));

            current_condition_ptr->operand2 = current_between_condition;
            step = stepWhereBetweenValue;
            break;
         }
         case stepWhereIn: 
         {
            current_in_condition = (in_condition_t *) malloc(sizeof(in_condition_t));
            current_in_condition->match_ptr = (list_node_t *) calloc(1, sizeof(list_node_t));
            current_in_value_ptr = current_in_condition->match_ptr;

            current_condition_ptr->operand2 = current_in_condition;
            step = stepWhereInValue;
            break;
         }
         case stepWhereEqualityValue:
         {
            current_euqality_condition->value = ptr;
            current_euqality_condition->value_type = SINGLE;

            ptr = strtok(NULL, delim);
            step = stepWhereValueType;
            break;
         }
         case stepWhereLikeValue: 
         {
            char *required_ex = malloc(strlen(ptr));
            strcpy(required_ex, ptr);
            required_ex[0] = '^';
            required_ex[strlen(required_ex) - 1] = '$';

            str_replace(required_ex, "%", ".*");

            regcomp(&(curent_like_condition->regex), required_ex, 0);

            // int reti = regexec(&(curent_like_condition->regex), "abc", 0, NULL, 0);
            // if (reti == 0) {
            //   printf("MATCH \n");
            // }
            // else {
            //   printf("NOT MATCH \n");
            // }

            ptr = strtok(NULL, delim);
            step = stepWhereContinue;

            break;
         }
         case stepWhereBetweenValue: 
         {
            char *between_value = malloc(strlen(ptr));
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
               current_between_condition->min_value_type = SINGLE;
            }
            else if (current_between_condition->max_value == NULL) {
               current_between_condition->max_value = ptr;
               current_between_condition->max_value_type = SINGLE;
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
            char *in_value = malloc(strlen(ptr));
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
            printf("in_value: %s\n", current_in_value_ptr->val);

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
                  arithmetic_condition_t *athm_condition = (arithmetic_condition_t *) malloc(sizeof(arithmetic_condition_t));
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
                        athm_condition->operand1 = (char *) current_euqality_condition->value;
                        
                        current_euqality_condition->value = athm_condition;
                        current_euqality_condition->value_type = ARITHMETIC;

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
               break;
            }
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
         default: 
         {
            ptr = strtok(NULL, delim);
            break;
         }
      }
   }

   return query;
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
   char operand_print[512];
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
         case SINGLE:
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
         case SINGLE:
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
      simple_condition_t *equality_condition = (simple_condition_t *) cond->operand2;
      switch (equality_condition->value_type)
      {
         case SINGLE:
         {
            strcpy(operand_print, (char *)(equality_condition->value));
            break;
         }
         case ARITHMETIC: 
         {
            arithmetic_condition_t *athm_cond = (arithmetic_condition_t *)(equality_condition->value);
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

   if (cond->not) 
   {
      printf("\"%s NOT %s %s\", ", cond->operand1, operator_to_print, operand_print);
   }
   else 
   {
      printf("\"%s %s %s\", ", cond->operand1, operator_to_print, operand_print);
   }
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
      printf("\"%s\", ", current_field->val);
      current_field = current_field->next;
   }
   printf("\n");

   printf("The tablename is \"%s\" \n", query->table_name);

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
}