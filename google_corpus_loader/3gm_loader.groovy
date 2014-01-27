
// Imports for GPARS
import groovyx.gpars.actor.Actor
import groovyx.gpars.actor.Actors
import groovyx.gpars.actor.DefaultActor
import groovyx.gpars.actor.BlockingActor

// Imports for Cassandra
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import static groovy.io.FileType.*;
import groovy.time.*

import com.datastax.driver.core.*;
import com.datastax.driver.core.exceptions.*

/*
 * The Gram Cleaner
 */

class SearchFilter {
    def characters_to_exclude
    def SearchFilter() {
        characters_to_exclude = populateExclusionArray()
    }
    def populateExclusionArray() {
        def c
        def exclusion_regexp = []
        def ex_char_end = ['\\)','\\(','/','"','\\.',':','>',';','\\+']
        ex_char_end += ['-{1,3}','\\*','!','@','\\]','\\^']
        def ex_char_start = ['[0-9]','<','\\+','-','\\*','\\(','@','=']
        ex_char_start += ['&',',','\\.','/',':',';','>',',','\\\'']
        ex_char_start += ['\\[',' \\.[0-9]',' [0-9]\\.[0-9:]']
        ex_char_start += [' [0-9:,-]']
        def ex_char = ['</S>','<S>','<UNK>','/']
        ex_char_end.each { p ->
            exclusion_regexp << ".*${p}\$".toString()
        }
        ex_char_start.each { p ->
            exclusion_regexp << "^${p}.*".toString()
        }
        ex_char.each { p ->
            exclusion_regexp << ".*${p}.*".toString()
        }
        print exclusion_regexp
        return exclusion_regexp
    }
    def exclude(String s) {
        for (regexp in characters_to_exclude) {
            def matcher = ( s =~ regexp )
            if (matcher.matches()) {
                //println "Match: Excluding ${s} with pattern ${regexp}"
                return true
            }
        }
        return false
    }
}

def sum(a) {
    def sum = a.inject(0) { acc, val ->
        acc + val
    }
    return sum
}

/*
 * LoadBalancer Class
 *
 * This is the main actor. It takes files from the queue (managed by
 * the fileActor) and passes them out to the workers that it spawns
 *
 */

final class LoadBalancer extends DefaultActor {
    int workers = 0
    List taskQueue = []
    def MAX_WORKERS
    def records = 0
    def start_time = System.currentTimeMillis()
    def test

    def LoadBalancer(test) {
        this.test = test
        if (test) {
            this.MAX_WORKERS = 4
        } else {
            this.MAX_WORKERS = 8
        }
    }

    void act() {
        loop {
            react { message ->
                switch (message) {
    		    case NeedMoreWork:
                        if (taskQueue.size() == 0) {
                            println "No more tasks in the queue - terminating worker"
                                reply DocumentWorker.EXIT
                                workers -= 1
                        } else reply taskQueue.remove(0)
                        break
                    case WorkToDo:
                        taskQueue << message
                        if (workers < MAX_WORKERS) {
                            println "Spawning More Workers"
                            workers += 1
                            new DocumentWorker(this, workers, test).start()
                        }
                        break
                    case ReportTaskQueue:
                        reply taskQueue.size()
                        break
                    case ReportWorkers:
                        reply workers
                        break
                    case ReportStatus:
                        reply(["queue_size":taskQueue.size(),"workers":workers])
                        break
                    case ReportRecordsInserted:
                        records = records + message.records
                        def current_time = System.currentTimeMillis()
                        def elapsed = current_time - start_time
                        println "Inserted: ${records} at ${records/elapsed} records/ms"
                        break
                    case FatalError:
                        println "Got a Fatal Error - shutting it down"
                        taskQueue = []
                        workers = 0
                        stop()
                        throw message.e
                }
            }
        }
    }
}

/*
 * This is the DocumentWorker
 *
 * It does the hard work of parsing the document and getting the list of triples
 *
 */
final class DocumentWorker extends DefaultActor {
    final static Object EXIT = new Object()

    Actor balancer
    def conn
    def term_frequency = [:]
    def id
    def sf
    def session
    def test
    Cluster cluster

    def DocumentWorker(balancer, id, test) {
        this.balancer = balancer
        this.id = id
        this.sf = new SearchFilter()
        this.test = test
        cluster = new Cluster.Builder().addContactPoints('127.0.0.1').build()
        session = cluster.connect()
    }

    void act() {
        loop {
            this.balancer << new NeedMoreWork()
            react {
                switch (it) {
                    case WorkToDo:
                        def records = processMessage(it.file_to_parse, it.document_id)
                        def report = new ReportRecordsInserted(records)
                        this.balancer << report
                        break
                    case EXIT: terminate()
                }
            }
        }
    }

    private int processMessage(file, document_id) {
        println "Working ${id} starting on ${document_id}"
        def table
        def table_lower
        def table_files
        def executions = 0
	// Hence ... 3 gram
        def number_of_terms = 3
        if (test) {
            table = "threegram_test"
            table_lower = "threegram_lower_test"
            table_files = "threegram_files_test"
        } else {
            table = "threegram"
            table_lower = "threegram_lower"
            table_files = "threegram_files"
        }
        int record = 0
        def records_reported = []
        def start_time = System.currentTimeMillis()
        def queries = ['BEGIN COUNTER BATCH']
        file.eachLine { line ->
            executions += 1
            def search
            def frequency
            (search, frequency) = line.split("\t")
            def exclude = sf.exclude(search)
            if (!exclude) {
                def raw_terms = search.split(" ")
                if (raw_terms.size() == number_of_terms) {
                    def terms = raw_terms.collect { item -> item.replace("'","''") }
                    def terms_lc = terms.collect { item -> item.toLowerCase() }
                    def query = "update fourgm.${table} set frequency = frequency + ${frequency} where gramone = '${terms[0]}' and gramtwo = '${terms[1]}' and gramthree = '${terms[2]}'"; 
                    def query_lc =  "update fourgm.${table_lower} set frequency = frequency + ${frequency} where gramone = '${terms_lc[0]}' and gramtwo = '${terms_lc[1]}' and gramthree = '${terms_lc[2]}'"; 
                    queries << query
                    queries << query_lc
                    record += 2
                    if (record != 0 && record % 3000 == 0) {
                        queries << "APPLY BATCH"
                        def q = queries.join("\n")
		        def tries = 0
                        while (1) {
    			    try {
    	                        session.execute(q)
                                break
                            } catch (NoHostAvailableException e) {
                                tries = tries + 1
                                if (tries > 4) {
                                    println "Worker ${id}: Insert failed after 5 tries, moving on"
                                    break
                                }
                                println "Worker ${id} No Cassandra Host Avaliable, backing off: ${tries} attempt(s)"
                                Thread.sleep(10000)
                            } catch (WriteTimeoutException e) {
                                println "Worker ${id} Query Consistency One Failed, coordinator will retry, backing off"
                                Thread.sleep(30000)
                            }
                        }
                        queries = ["BEGIN COUNTER BATCH"]
                    }
                }
            }
            // Give a rate update
            if (record != 0 && record % 1000000 == 0 && !(record in records_reported)) {
                records_reported << record
                def current_time = System.currentTimeMillis()
                def thread_id = Thread.currentThread().getId()
                println "Thread: ${thread_id} Worker: ${id} Records: ${record} Executions: ${executions} Rate: ${record / (current_time - start_time)}"
            }
        }
        if (queries.size() > 1) {
            queries << "APPLY BATCH"
            def q = queries.join("\n")
            session.execute(q)           
        }
        // Update our control table
        def query = "insert into fourgm.${table_files} (fileid, records, lines) values ('${document_id}', ${record}, ${executions})"
        session.execute(query)
        println "Worker ${id} finishing ${document_id} with ${record} records"
        return record
    }
}



/*
 * These classes are just used to pass messages back and forth
 * between the actors
 */

final class WorkToDo {
    def file_to_parse
    def document_id

    def WorkToDo(file, document_id) {
        this.file_to_parse = file
        this.document_id = document_id
    }
}
final class ReportRecordsInserted {
    def records

    def ReportRecordsInserted(records) {
        this.records = records
    }
}
final class NeedMoreWork{}
final class ReportTaskQueue{}
final class ReportWorkers{}
final class ReportStatus{}
final class WorkerExit{}
final class FatalError {
    def e
    def FatalError(e) {
        this.e = e
    }
}


/*
 * Main is here
 *
 */
def test = false

final Actor balancer = new LoadBalancer(test).start()

// Here is our director of files to parse
def base_dir
if (test) {
    base_dir = new File("./data_3gm_test")
} else {
    base_dir = new File("./data_3gm")
} 

files = []
p = ~/3gm-.*/
base_dir.eachFileMatch(p) { f ->
    f.eachFile() { datafile ->
        files << datafile
    }
}

if (files.size() == 0) {
    println "No files found in the specified director"
    System.exit(1)
}

/* Files Actor
 * 
 * Not much to see here. Load files, queue them up, pass them to parser workers
 */
def fileActor = Actors.actor {
    def queue_size = 1
    def document_id
    files.each { file -> 
        while (queue_size > 20) {
            println "Queue: ${queue_size} .. sleeping"
            Thread.sleep 100000
            // Get the queue size
            def message = balancer.sendAndWait(new ReportStatus())
            queue_size = message.queue_size
        }           
        def file_name = file.toString()
        def filepattern = file_name =~ /3gm-([0-9]+)/
        document_id = filepattern[0][1]
        println "Adding ${document_id}"
        balancer << new WorkToDo(file, document_id)
        // Get the queue size
        def message = balancer.sendAndWait(new ReportStatus())
        queue_size = message.queue_size
    }
    println "Finished Queuing Documents: Last Document ${document_id}"


    // We have processed all of the files .. wait for the load balancer
    // to finish
    while (true) {
        def message = balancer.sendAndWait(new ReportStatus())
        if (message.queue_size == 0 && message.workers == 0) {
            println "Done"
            stop()
            break
        } else {
            // Still waiting
            Thread.sleep 10000
        }
    }
}

fileActor.join()

println "All Done"
System.exit(0)
