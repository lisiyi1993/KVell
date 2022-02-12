#include "headers.h"
#include "hashtable.h"

volatile int _commit_done = 0;
void commit_done(struct slab_callback *cb, void *item) {
   printf("Just committed transaction %lu\n", get_transaction_id(cb->transaction));
   _commit_done = 1;
   free(cb->transaction);
   free(cb);
}

int commit(struct transaction *t, struct injector_queue *q) {
   struct slab_callback *new_cb = new_slab_callback();
   new_cb->cb = commit_done;
   new_cb->transaction = t;
   new_cb->injector_queue = q;
   _commit_done = 0;
   return kv_commit(t, new_cb);
}

void cb_trans(struct slab_callback *cb, void *item) {
   if(item) {
      printf("\t[%lu] Just read an item (status: %s): ", get_transaction_id(cb->transaction), has_failed(cb->transaction)?"failed":"success");
      print_item(cb->action, item);
   } else {
      printf("\t[%lu] Just wrote an item (status: %s): ", get_transaction_id(cb->transaction), has_failed(cb->transaction)?"failed":"success");
      print_item(cb->action, cb->item);
   }

   free(cb->item);
   free(cb);
}

struct slab_callback *t_bench_cb(void) {
   struct slab_callback *cb = new_slab_callback();
   cb->cb = cb_trans;
   return cb;
}

int main(int argc, char **argv) {
   char lineitem_columns[][100] = {
      "TABLE", "ORDERKEY", "PARTKEY", "SUPPKEY", "LINENUMBER", "QUANTITY", "EXTENDEDPRICE", "DISCOUNT", "TAX", "RETURNFLAG", "LINESTATUS", "SHIPDATE", "COMMITDATE", "RECEIPTDATE", "SHIPINSTRUCT", "SHIPMODE", "COMMENT"
   };
   char lineitem_columns_type[][100] = {
      "STRING", "INT", "INT", "INT", "INT", "INT", "INT", "INT", "INT", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING"
   };

   char orders_columns[][100] = {
      "TABLE", "ORDERKEY", "ORDERDATE", "SHIPPRIORITY"
   };
   char orders_columns_type[][100] = {
      "STRING", "INT", "STRING", "STRING"
   };

   create_sql_tables_columns();
   create_table_identifier_to_table_name();
   create_lineitem_table(sizeof(lineitem_columns)/100, lineitem_columns, lineitem_columns_type);
   create_orders_table(sizeof(orders_columns)/100, orders_columns, orders_columns_type);

   // char input_sql[] = "SELECT QUANTITY , TAX , DISCOUNT , RETURNFLAG , SHIPDATE FROM table WHERE SHIPDATE LIKE '1998%' AND DISCOUNT BETWEEN 0 AND 0 + 500";
   char input_sql[] = "SELECT l_ORDERKEY , l_TAX , l_DISCOUNT , l_RETURNFLAG , l_SHIPDATE FROM lineitem , orders WHERE o_ORDERKEY = l_ORDERKEY";
   // char input_sql[] = "SELECT a_QUANTITY , a_TAX , a_DISCOUNT , a_RETURNFLAG FROM a_t1 , b_t2 WHERE a_DISCOUNT = b_DISCOUNT";
   origin_query = parse_sql(input_sql);

   printf("\nOrigin Query object\n");
   print_query_object(origin_query);
   printf("\n");

   query_t *sub_query_list[2];
   int index = 0;

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

         // printf("%s ", current_field_ptr->val);
         hashset_iterator_next(table_columns_iter);
         if (hashset_iterator_has_next(table_columns_iter))
         {
            current_field_ptr->next = calloc(1, sizeof(list_node_t));
            current_field_ptr = current_field_ptr->next;
         }
      }

      // add conditions too
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
               current_sub_query_and_condition_ptr = (condition_t *) calloc(1, sizeof(condition_t));
            }

            current_sub_query_and_condition_ptr->operand1 = origin_and_condition_ptr->operand1;
            current_sub_query_and_condition_ptr->operator = origin_and_condition_ptr->operator;
            current_sub_query_and_condition_ptr->operand2 = origin_and_condition_ptr->operand2;
            current_sub_query_and_condition_ptr->not = origin_and_condition_ptr->not;

            current_sub_query_and_condition_ptr = current_sub_query_and_condition_ptr->next_condition;
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
               current_sub_query_or_condition_ptr = (condition_t *) calloc(1, sizeof(condition_t));
            }

            current_sub_query_or_condition_ptr->operand1 = origin_or_condition_ptr->operand1;
            current_sub_query_or_condition_ptr->operator = origin_or_condition_ptr->operator;
            current_sub_query_or_condition_ptr->operand2 = origin_or_condition_ptr->operand2;
            current_sub_query_or_condition_ptr->not = origin_or_condition_ptr->not;
            
            current_sub_query_or_condition_ptr = current_sub_query_or_condition_ptr->next_condition;
         }
         origin_or_condition_ptr = origin_or_condition_ptr->next_condition;
      }

      ht_set(sub_queries_map, selected_table_identifier, current_sub_query);
   }

   // hti sub_queries_map_iterator = ht_iterator(sub_queries_map);
   // while (ht_next(&sub_queries_map_iterator))
   // {
   //    char *table_identifier = (char *) sub_queries_map_iterator.key;
   //    query_t *sub_query = (query_t *) sub_queries_map_iterator.value;
   //    printf("\nThe query object for table %s\n", table_identifier);
   //    print_query_object(sub_query);
   // }

   // printf("\ntable columns object result\n");
   // hti sql_tables_columns_iterator = ht_iterator(sql_tables_columns);
   // while (ht_next(&sql_tables_columns_iterator)) {
   //    char *selected_table_identifier = (char *) sql_tables_columns_iterator.key;
   //    hashset_t table_columns = (hashset_t) sql_tables_columns_iterator.value;
   //    hashset_itr_t iter = hashset_iterator(table_columns);

   //    sub_query_list[index] = (query_t *) calloc(1, sizeof(query_t));
   //    sub_query_list[index]->type = SELECT;
   //    sub_query_list[index]->table_name_ptr = (table_name_t *) calloc(1, sizeof(table_name_t));
   //    sub_query_list[index]->table_name_ptr->name = ht_get(table_identifier_to_table_name, selected_table_identifier);
   //    strcpy(sub_query_list[index]->table_name_ptr->identifier, selected_table_identifier);
      
   //    sub_query_list[index]->field_ptr = calloc(1, sizeof(list_node_t));
   //    list_node_t *current_field_ptr = sub_query_list[index]->field_ptr;

   //    printf("%s ->", selected_table_identifier);
   //    while(hashset_iterator_has_next(iter))
   //    {
   //       current_field_ptr->val = (char *) hashset_iterator_value(iter);
   //       current_field_ptr->key = (char *) sql_tables_columns_iterator.key;

   //       printf(" %s", current_field_ptr->val);
   //       hashset_iterator_next(iter);
   //       if (hashset_iterator_has_next(iter))
   //       {
   //          current_field_ptr->next = calloc(1, sizeof(list_node_t));
   //          current_field_ptr = current_field_ptr->next;
   //       }
   //    }

   //    printf("\n");
   //    print_query_object(sub_query_list[index]);
   //    index++;
   //    hashset_destroy(table_columns);
   //    printf("\n");
   // }

   int nb_disks, nb_workers_per_disk;
   declare_timer;

   /* Definition of the workload, if changed you need to erase the DB before relaunching */
   struct workload w = {
      .api = &SQL_PARSER,
      .nb_items_in_db = get_db_size_sql_parser(),
      .nb_load_injectors = 1
   };


   /* Parsing of the options */
   if(argc < 3)
      die("Usage: ./main <nb disks> <nb workers per disk>\n\tData is stored in %s\n", PATH);
   nb_disks = atoi(argv[1]);
   nb_workers_per_disk = atoi(argv[2]);

   /* Pretty printing useful info */
   printf("# Configuration:\n");
   printf("# \tPage cache size: %lu GB\n", PAGE_CACHE_SIZE/1024/1024/1024);
   printf("# \tWorkers: %d working on %d disks\n", nb_disks*nb_workers_per_disk, nb_disks);
   printf("# \tIO configuration: %d queue depth (capped: %s)\n", QUEUE_DEPTH, NEVER_EXCEED_QUEUE_DEPTH?"yes":"no");
   printf("# \tQueue configuration: %d maximum pending callbaks per worker\n", MAX_NB_PENDING_CALLBACKS_PER_WORKER);
   printf("# \tThread pinning: %s spinning: %s\n", PINNING?"yes":"no", SPINNING?"yes":"no");
   printf("# \tBench: %s (%lu elements)\n", w.api->api_name(), w.nb_items_in_db);
   printf("# \tSnapshot: %s\n", TRANSACTION_TYPE==TRANS_LONG?"BSI":"SI");

   /* Initialization of random library */
   start_timer {
      init_transaction_manager();
      printf("Initializing random number generator (Zipf) -- this might take a while for large databases...\n");
      init_zipf_generator(0, w.nb_items_in_db - 1); /* This takes about 3s... not sure why, but this is legacy code :/ */
   } stop_timer("Initializing random number generator (Zipf)");

   /* Recover database */
   start_timer {
      slab_workers_init(nb_disks, nb_workers_per_disk);
   } stop_timer("Init found %lu elements", get_database_size());
   
   /* Add missing items if any */
   repopulate_db(&w);
   while(pending_work()); // Wait for the DB to be fully populated

   usleep(500000);

   /* Launch benchs */
   bench_t workload, workloads[] = {
      sql_parser,
   };

   int nb_scan, nb_scans[] = {1};
   foreach(workload, workloads) {
      foreach(nb_scan, nb_scans) {
         w.nb_requests = 1;
         w.nb_scans = nb_scan;
         printf("LAUNCHING WITH %d SQL PARSER\n", nb_scan);
         run_workload(&w, workload);
      }
   }
   
   ht_destroy(lineitem_table->column_map);
   ht_destroy(sql_tables_columns);
   return 0;
}

