/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.activemq.syscall.util;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import org.apache.activemq.syscall.FileDescriptor;
import org.apache.activemq.syscall.NativeAllocation;
import org.apache.activemq.util.cli.CommonsCLISupport;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import static java.util.concurrent.TimeUnit.*;
import static org.apache.activemq.syscall.jni.IO.*;
import static org.apache.activemq.util.cli.OptionBuilder.*;

/**
 * This class is used to get a benchmark the raw disk performance.
 */
public class DiskBenchmark {

    private static final HashSet<String> OPS = new HashSet<String>();
    private static String op(String v) {
        OPS.add(v);
        return v;
    }
    private static final String WRITE_OP = op("write");
    private static final String READ_OP = op("read");
    
    private static final HashSet<String> APIS = new HashSet<String>();
    private static String api(String v) {
        APIS.add(v);
        return v;
    }
    private static final String NATIVE_BIO_API = api("native-bio");
    private static final String JAVA_API = api("java");
    
    boolean verbose;
    // reads and writes work with 4k of data at a time.
    int blockSize=1024*4; 
    long blocks=128000;
    String file;
    int sampleCount = 10;
    double samplePeriod = 1;
    
    String[] api = { JAVA_API, NATIVE_BIO_API };
    String[] operation = { WRITE_OP };
    boolean[] random={ false };
    boolean[] memcopy={ false };
    boolean[] sync={ false };
    
    static private Options createOptions() {
        Options options = new Options();
        options.addOption("h", "help",    false, "Display help information");
        options.addOption("v", "verbose", false, "Verbose");

        options.addOption(ob()
                .id("f")
                .name("file")
                .arg("path")
                .description("").op());

        options.addOption(ob()
                .id("s")
                .name("block-size")
                .arg("bytes")
                .description("").op());

        options.addOption(ob()
                .id("b")
                .name("blocks")
                .arg("count")
                .description("").op());

        options.addOption(ob()
                .id("c")
                .name("sample-count")
                .arg("number")
                .description("").op());
        
        options.addOption(ob()
                .id("p")
                .name("sample-period")
                .arg("seconds")
                .description("").op());

        options.addOption(ob()
                .name("sync")
                .args(2)
                .sperator(',')
                .arg("true,false")
                .description("").op());

        return options;
    }


    public static void main(String[] args) {
        String jv = System.getProperty("java.version").substring(0, 3);
        if (jv.compareTo("1.5") < 0) {
            System.err.println("This application requires jdk 1.5 or higher to run, the current java version is " + System.getProperty("java.version"));
            System.exit(-1);
            return;
        }

        DiskBenchmark app = new DiskBenchmark();
        System.exit(app.execute(args));
    }
    
    ///////////////////////////////////////////////////////////////////
    // Entry point for an embedded users who want to call us with
    // via command line arguments.
    ///////////////////////////////////////////////////////////////////
    public int execute(String[] args) {
        CommandLine cli = null;
        try {
            cli = new PosixParser().parse(createOptions(), args, true);
        } catch (ParseException e) {
            System.err.println( "Unable to parse command line options: " + e.getMessage() );
            displayHelp();
            return 1;
        }

        if( cli.hasOption("h") ) {
            displayHelp();
            return 0;
        }
        
        DiskBenchmark benchmark = new DiskBenchmark();
        args = CommonsCLISupport.setOptions(benchmark, cli);
        try {
            benchmark.benchmark();
            return 0;
        } catch (UsageException e) {
            System.out.println("Invalid Usage: "+e.getMessage());
            displayHelp();
            return -1;
        } catch (Throwable e) {
            System.out.flush();
            if (benchmark.verbose) {
                System.err.println("ERROR:");
                e.printStackTrace();
            } else {
                System.err.println("ERROR: " + e);
            }
            System.err.flush();
            return -2;
        }
    }
    
    @SuppressWarnings("serial")
    static class UsageException extends Exception {
        public UsageException(String msg) {
            super(msg);
        }
    }
    
    public void benchmark() throws IOException, UsageException {
        if( file==null ) {
            throw new UsageException("file not specified.");
        }
        
        for (String v : api) {
            if( !APIS.contains(v) ) {
                throw new UsageException("invalid api: "+v);
            }
        }
        for (String v : operation) {
            if( !OPS.contains(v) ) {
                throw new UsageException("invalid operation: "+v);
            }
        }
        
        PrintWriter pw = new PrintWriter(new FileOutputStream("report.html"));
        writeReportHeader(pw);
        int chartCounter=0;
        for (boolean memcopy : this.memcopy) {
            for (String operation : this.operation) {
                for (boolean random : this.random) {
                    for (boolean sync : this.sync) {
                        ArrayList<Benchmark> results = new ArrayList<Benchmark>(this.api.length); 
                        for (String apiName : this.api) {
                            Benchmark benchmark = createBenchmark(apiName);
                            benchmark.random = random;
                            benchmark.sync = sync;
                            benchmark.memcopy = memcopy;
                            benchmark.apiName = apiName;
                            benchmark.operation = operation;
                            benchmark.execute();
                            results.add(benchmark);
                        }
                        writeReportChart(pw, chartCounter++, results);
                    }
                }
            }
        }
        
        writeReportFooter(pw);
        pw.close();

    }

    private void writeReportHeader(PrintWriter pw) {
        if(pw==null) 
            return;
        pw.println("<html>");
        pw.println("  <head>");
        pw.println("    <script type='text/javascript' src='http://www.google.com/jsapi'></script>");
        pw.println("    <script type='text/javascript'>");
        pw.println("      google.load('visualization', '1', {'packages':['linechart']});");
        pw.println("    </script>");
        pw.println("    <style type='text/css'>");
        pw.println("      .chart-section {width:640px; padding-left: 30px}");
        pw.println("      .chart-props {width:140px; padding:0; padding-top:20px; float:left;}");
        pw.println("      .chart-graph {width: 500px; height: 200px; float:right; }");
        pw.println("    </style>");
        pw.println("  </head>");
        pw.println("  <body>");
    }
    
    private void writeReportChart(PrintWriter pw, int id, ArrayList<Benchmark> data) {
        if(pw==null) 
            return;
        Benchmark d1 = data.get(0);
        
        String titleX = String.format("Period (%.2f seconds)", samplePeriod);
        String titleY = "IO Operations";
        
        pw.println("    <div class='chart-section'>");
        pw.println("      <ul class='chart-props'>");
        pw.println("        <li>operation: "+d1.operation+"</li><li>sync: "+d1.sync+"</li><li>memcopy: "+d1.memcopy+"</li><li>random: "+d1.random+"</li>");
        pw.println("      </ul>");
        pw.println("      <div id='chart_"+id+"' class='chart-graph '></div>");
        pw.println("    </div>");
        pw.println("    <script type='text/javascript'>");
        pw.println("      google.setOnLoadCallback(draw_chart_"+id+");");
        pw.println("      function draw_chart_"+id+"() {");
        pw.println("        var data = new google.visualization.DataTable();");
        pw.println("        data.addColumn('string', 'Period');");
        
        for (Benchmark d : data) {
            pw.println("        data.addColumn('number', '"+d.apiName+"');");
        }
        pw.println("        data.addRows([");
        for( int i=0; i < sampleCount; i++ ) {
            if( i!=0 ) {
                pw.println(",");
            }
            pw.print("          ['"+i+"'");
            for (Benchmark d : data) {
                long value = 0;
                if( d.samples.size() >i ) {
                    value = d.samples.get(i);
                }
                pw.print(", "+value);
            }
            pw.print("]");
        }
        
        pw.println("        ]);");
        pw.println("        var chart = new google.visualization.LineChart(document.getElementById('chart_"+id+"'));");
        pw.println("        chart.draw(data, {legend: 'bottom', smoothLine:true, titleX:'"+titleX+"', titleY:'"+titleY+"' });");
        pw.println("      }");
        pw.println("    </script>");
        
    }

    private String title(ArrayList<Benchmark> data) {
        Benchmark d = data.get(0);
        return "operation: "+d.operation+", sync: "+d.sync+", memcopy: "+d.memcopy+", random: "+d.random;
    }


    private void writeReportFooter(PrintWriter pw) {
        if(pw==null) 
            return;
        pw.println("  </body>");
        pw.println("</html>");
    }

    private Benchmark createBenchmark(String api) {
        if( JAVA_API.equals(api) ) 
            return new JavaApi();
        if( NATIVE_BIO_API.equals(api) ) 
            return new NativeBioApi();
        throw new RuntimeException("Unsupported API: "+api);
    }

    static public class Sampler extends Thread {
        private final AtomicReference<ArrayList<Long>> samples = new AtomicReference<ArrayList<Long>>();
        private final AtomicLong metric;
        private final int count;
        private final long period;
        public boolean verbose;
        
        public Sampler(AtomicLong metric, int count, double periodInSecs) {
            super("Sampler");
            this.metric = metric;
            this.count = count;
            this.period = ns(periodInSecs);
            setPriority(MAX_PRIORITY);
            setDaemon(true);
        }
        
        static private long ns(double v) {
            return (long)(v*NANOSECONDS.convert(1, SECONDS));
        }

        @Override
        public void run() {
            ArrayList<Long> samples = new ArrayList<Long>(count);
            try {
                long sleepMS = period/1000000;
                int sleepNS = (int) (period%1000000);
                long currentValue;
                long lastValue = metric.get();
                for (int i = 0; i < count; i++) {
                    if( verbose ) {
                        System.out.print(".");
                    }
                    Thread.sleep(sleepMS,sleepNS);
                    currentValue = metric.get();
                    samples.add(currentValue-lastValue);
                    lastValue=currentValue;
                }
            } catch (InterruptedException e) {
            } finally {
                if( verbose ) {
                    System.out.println();
                }
                this.samples.set(samples);
            }
        }
        
        public synchronized ArrayList<Long> getSamples() {
            return samples.get();
        }
    }
    
    abstract class Benchmark {
        
        public String apiName;
        public String operation;
        public boolean random; 
        public boolean sync;
        public boolean memcopy;
        public ArrayList<Long> samples;
        
        final public void execute() throws IOException {
            
            boolean write;
            if( WRITE_OP.equals(operation) ) {
                write = true;
            } else if( READ_OP.equals(operation) ) {
                write = false;
            } else {
                throw new RuntimeException();
            }
            
            System.out.print("Benchmarking: api: "+apiName+", operation: "+operation+", random: "+random+", sync: "+sync+", memcopy: "+memcopy+" ");

            AtomicLong ioCount=new AtomicLong();
            Sampler sampler = new Sampler(ioCount, sampleCount, samplePeriod);
            sampler.verbose = verbose;
            try {
                init(true);
                sampler.start();
                outer: while( true ) {
                    seek(0);
                    for( long i=0; i < blocks; i++) {
                        if( write ) {
                            write();
                            if( sync ) {
                                sync();
                            }
                        } else {
                            read();
                        }
                        ioCount.incrementAndGet();
                        if( !sampler.isAlive() ) {
                            break outer;
                        }
                    }
                }
            } finally {
                dispose();
            }
            samples = sampler.getSamples();
            System.out.println("  results: "+samples);
            System.out.println();

        }

        abstract protected void init(boolean write) throws IOException;
        abstract protected void dispose() throws IOException;
        abstract protected void seek(long pos) throws IOException;
        abstract protected void write() throws IOException;
        abstract protected void read() throws IOException;
        abstract protected void sync() throws IOException;
        
    }
    
    class NativeBioApi extends Benchmark {
        
        private FileDescriptor fd;
        private NativeAllocation data;

        @Override
        protected void init(boolean write) throws IOException {
            data = NativeAllocation.allocate(block());
            if( write ) {
                int oflags =  O_CREAT | O_TRUNC | O_WRONLY;
                int mode = S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH;
                fd = FileDescriptor.open(file, oflags, mode);
            } else {
                int oflags =  O_RDONLY;
                fd = FileDescriptor.open(file, oflags);
            }
        }

        @Override
        protected void dispose() throws IOException {
            if( data!=null ) 
                data.free();
            if( fd!=null ) 
                fd.close();
        }

        @Override
        protected void seek(long pos) throws IOException {
            fd.seek(pos);
        }

        @Override
        protected void sync() throws IOException {
            fd.sync();
        }

        @Override
        protected void write() throws IOException {
            fd.write(data);
        }
        
        @Override
        protected void read() throws IOException {
            fd.read(data);
        }
    }
    
    class JavaApi extends Benchmark {
        
        private RandomAccessFile raf;
        private byte data[];

        @Override
        protected void init(boolean write) throws IOException {
            data = block();
            if( write ) {
                raf = new RandomAccessFile(file, "rw");
            } else {
                raf = new RandomAccessFile(file, "r");
            }
        }

        @Override
        protected void dispose() throws IOException {
            if( raf!=null ) 
                raf.close();
        }

        @Override
        protected void seek(long pos) throws IOException {
            raf.seek(pos);
        }

        @Override
        protected void sync() throws IOException {
            raf.getFD().sync();
        }

        @Override
        protected void write() throws IOException {
            raf.write(data);
        }
        
        @Override
        protected void read() throws IOException {
            raf.readFully(data);
        }
    }
    
    private byte[] block() {
        byte []data = new byte[blockSize];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte)('a'+(i%26));
        }
        return data;
    }

    private void displayHelp() {
        System.err.flush();
        String app = System.getProperty("diskbenchmark.application");
        if( app == null ) {
            try {
                URL location = getClass().getProtectionDomain().getCodeSource().getLocation();
                String[] split = location.toString().split("/");
                if( split[split.length-1].endsWith(".jar") ) {
                    app = split[split.length-1];
                }
            } catch (Throwable e) {
            }
            if( app == null ) {
                app = getClass().getSimpleName();
            }
        }

        // The commented out line is 80 chars long.  We have it here as a visual reference
//      p("                                                                                ");
        p();
        p("Usage: "+ app +" [options]");
        p();
        p("Description:");
        p();
        pw("  "+app+" is a disk benchmarker.", 2);
        p();

        p("Options:");
        p();
        PrintWriter out = new PrintWriter(System.out);
        HelpFormatter formatter = new HelpFormatter();
        formatter.printOptions(out, 78, createOptions(), 2, 2);
        out.flush();
        p();
    }
    

    private void p() {
        System.out.println();
    }
    private void p(String s) {
        System.out.println(s);
    }
    private void pw(String message, int indent) {
        PrintWriter out = new PrintWriter(System.out);
        HelpFormatter formatter = new HelpFormatter();
        formatter.printWrapped(out, 78, indent, message);
        out.flush();
    }


    public void setVerbose(boolean verbose) {
        this.verbose = verbose;
    }
    public void setBlockSize(int blockSize) {
        this.blockSize = blockSize;
    }
    public void setBlocks(long blocks) {
        this.blocks = blocks;
    }
    public void setFile(String file) {
        this.file = file;
    }
    public void setSampleCount(int sampleCount) {
        this.sampleCount = sampleCount;
    }
    public void setSamplePeriod(double samplePeriod) {
        this.samplePeriod = samplePeriod;
    }
    public void setApi(String[] api) {
        this.api = api;
    }
    public void setOperation(String[] operation) {
        this.operation = operation;
    }
    public void setRandom(boolean[] random) {
        this.random = random;
    }
    public void setMemcopy(boolean[] memcopy) {
        this.memcopy = memcopy;
    }
    public void setSync(boolean[] sync) {
        this.sync = sync;
    }
    
}
