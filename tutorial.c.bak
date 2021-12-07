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
   char input_columns[][100] = {
      "ORDERKEY", "PARTKEY", "SUPPKEY", "LINENUMBER", "QUANTITY", "EXTENDEDPRICE", "DISCOUNT", "TAX", "RETURNFLAG", "LINESTATUS", "SHIPDATE", "COMMITDATE", "RECEIPTDATE", "SHIPINSTRUCT", "SHIPMODE", "COMMENT"
   };

   char input_columns_type[][100] = {
      "INT", "INT", "INT", "INT", "INT", "INT", "INT", "INT", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING"
   };

   test_table = calloc(1, sizeof(test_table));
   test_table->name = "LINEITEM";
   test_table->column_map = ht_create();
   for (size_t i = 0; i < sizeof(input_columns)/100; i++)
   {
      // int *index = malloc(sizeof(int));
      // *index = i;

      struct column_info *ci = malloc(sizeof(ci));
      ci->index = i;

      if (strcmp(input_columns_type[i], "INT") == 0) {
         ci->type = INT;
      }
      else if (strcmp(input_columns_type[i], "STRING") == 0)
      {
         ci->type = STRING;
      }
      
      // if (test_table->column_map == NULL) {
      //    printf("yes it is null\n");
      // }
      ht_set(test_table->column_map, input_columns[i], ci);
      // struct column_info *ci_get = ht_get(test_table->column_map, input_columns[i]);
      // printf("here \n");
      // printf("%s and (%d)\n", input_columns[i], ci_get->index);
      // printf("done\n");
   }

   // char input_sql[] = "SELECT QUANTITY , TAX , DISCOUNT , RETURNFLAG , SHIPDATE FROM table WHERE SHIPDATE LIKE '1998-11%' OR RETURNFLAG NOT LIKE 'A'";
   char input_sql[] = "SELECT QUANTITY , TAX , DISCOUNT , RETURNFLAG , SHIPDATE FROM table WHERE RETURNFLAG IN ('A' , 'C') OR SHIPDATE OR LIKE '1998-09%' OR DISCOUNT > 100";
   query = parse_sql(input_sql);
   print_query_object(query);

   // int nb_disks, nb_workers_per_disk;
   // declare_timer;

   // /* Definition of the workload, if changed you need to erase the DB before relaunching */
   // struct workload w = {
   //    .api = &SQL_PARSER,
   //    .nb_items_in_db = get_db_size_sql_parser(),
   //    .nb_load_injectors = 1
   // };


   // /* Parsing of the options */
   // if(argc < 3)
   //    die("Usage: ./main <nb disks> <nb workers per disk>\n\tData is stored in %s\n", PATH);
   // nb_disks = atoi(argv[1]);
   // nb_workers_per_disk = atoi(argv[2]);

   // /* Pretty printing useful info */
   // printf("# Configuration:\n");
   // printf("# \tPage cache size: %lu GB\n", PAGE_CACHE_SIZE/1024/1024/1024);
   // printf("# \tWorkers: %d working on %d disks\n", nb_disks*nb_workers_per_disk, nb_disks);
   // printf("# \tIO configuration: %d queue depth (capped: %s)\n", QUEUE_DEPTH, NEVER_EXCEED_QUEUE_DEPTH?"yes":"no");
   // printf("# \tQueue configuration: %d maximum pending callbaks per worker\n", MAX_NB_PENDING_CALLBACKS_PER_WORKER);
   // printf("# \tThread pinning: %s spinning: %s\n", PINNING?"yes":"no", SPINNING?"yes":"no");
   // printf("# \tBench: %s (%lu elements)\n", w.api->api_name(), w.nb_items_in_db);
   // printf("# \tSnapshot: %s\n", TRANSACTION_TYPE==TRANS_LONG?"BSI":"SI");

   // /* Initialization of random library */
   // start_timer {
   //    init_transaction_manager();
   //    printf("Initializing random number generator (Zipf) -- this might take a while for large databases...\n");
   //    init_zipf_generator(0, w.nb_items_in_db - 1); /* This takes about 3s... not sure why, but this is legacy code :/ */
   // } stop_timer("Initializing random number generator (Zipf)");

   // /* Recover database */
   // start_timer {
   //    slab_workers_init(nb_disks, nb_workers_per_disk);
   // } stop_timer("Init found %lu elements", get_database_size());
   
   // /* Add missing items if any */
   // repopulate_db(&w);
   // while(pending_work()); // Wait for the DB to be fully populated

   // usleep(500000);

   // /* Launch benchs */
   // bench_t workload, workloads[] = {
   //    sql_parser,
   // };

   // int nb_scan, nb_scans[] = {1};
   // foreach(workload, workloads) {
   //    foreach(nb_scan, nb_scans) {
   //       w.nb_requests = 1;
   //       w.nb_scans = nb_scan;
   //       printf("LAUNCHING WITH %d SQL PARSER\n", nb_scan);
   //       run_workload(&w, workload);
   //    }
   // }
   
   // ht_destroy(test_table->column_map);
   return 0;
}

