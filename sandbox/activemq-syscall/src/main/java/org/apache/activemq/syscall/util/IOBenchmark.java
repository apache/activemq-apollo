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

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.PrintWriter;
import java.io.RandomAccessFile;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Random;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.activemq.syscall.Callback;
import org.apache.activemq.syscall.FileDescriptor;
import org.apache.activemq.syscall.FutureCallback;
import org.apache.activemq.syscall.NativeAllocation;
import org.apache.activemq.util.Combinator;
import org.apache.activemq.util.IOExceptionSupport;
import org.apache.activemq.util.IntrospectionSupport;
import org.apache.activemq.util.MemoryPropertyEditor;
import org.apache.activemq.apollo.util.cli.CommonsCLISupport;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.cli.PosixParser;

import static java.lang.String.*;
import static org.apache.activemq.syscall.jni.IO.*;
import static org.apache.activemq.util.cli.OptionBuilder.*;

/**
 * This class is used to get a benchmark the raw disk performance.
 */
public class IOBenchmark {

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
    private static final String JAVA_API = api("java");
    private static final String NATIVE_BIO_API = api("native-bio");
    private static final String NATIVE_AIO_DIRECT_API = api("native-aio-direct");
    
    boolean verbose;

    String blockSize="4k";
    int blockSizeInBytes=1024*4;

    String fileSize="500M";
    long blocks=128000;
    
    String file = "benchmark.bin";
    String report = "benchmark.html";
    int sampleCount = 5;
    double samplePeriod = 2;
    
    String[] api = { JAVA_API, NATIVE_BIO_API, NATIVE_AIO_DIRECT_API };
    String[] operation = { READ_OP, WRITE_OP };
    
    // read|write options
    boolean[] randomSeek={ false };
    
    // read options
    boolean[] keepDirect={ true };
    
    // write options
    boolean[] sync={ false };
    boolean[] preallocate={ true };
    
    private Options createOptions() {
        Options options = new Options();

        options.addOption("h", "help",    false, "Display help information");
        options.addOption("v", "verbose", false, "Verbose");

        options.addOption(ob().id("d").name("file").arg("path")
                .description("Path to the data file. Default: "+file).op());
        options.addOption(ob().id("r").name("report").arg("path")
                .description("Path to the report html that will be generated. Default: "+report).op());
        options.addOption(ob().id("fs").name("file-size").arg("count")
                .description("Size of the data file.  The larger you make it the more likely that the OS will not be able to cache the data. Default: "+fileSize).op());
        options.addOption(ob().id("bs").name("block-size").arg("bytes")
                .description("The size of each benchmarked io operation. Default: "+blockSize).op());
        options.addOption(ob().id("sc").name("sample-count").arg("number")
                .description("How many samples should the bench mark take of each run. Default: "+sampleCount).op());
        options.addOption(ob().id("sp").name("sample-period").arg("seconds")
                .description(format("How many seconds should elapse between samples. Default: %.2f", samplePeriod)).op());
        
        options.addOption(ob().id("a").name("api").arg("type,type").args(2).sperator(',')
                .description("Which api style should get benchmarked.  Pick from: "+APIS+".  Default: "+join(api, ",")).op());
        options.addOption(ob().id("o").name("operation").arg("value,value").args(2).sperator(',')
                .description("The type of io operation that should be benchmarked.  Default: "+join(operation, ",")).op());
        
        // read|write options
        options.addOption(ob().id("rs").name("random-seek").arg("true,false").args(2).sperator(',')
                .description("Should a random seek be done before the io operation? Affects: read, write. Default: "+join(randomSeek, ",")).op());
        options.addOption(ob().id("kd").name("keep-direct").arg("true,false").args(2).sperator(',')
                .description("Can direct/native operations skip converting native buffers to java byte arrays?  Affects: read, write. Default: "+join(keepDirect, ",")).op());

        // write options
        options.addOption(ob().id("pr").name("preallocate").arg("true,false").args(2).sperator(',')
                .description("Should the data file be preallocated before running the benchmark? Affects: write. Default: "+join(preallocate, ",")).op());
        options.addOption(ob().id("sy").name("sync").arg("true,false").args(2).sperator(',')
                .description("Should a file sync be performed after each io operation? Affects: write. Default: "+join(preallocate, ",")).op());

        return options;
    }


    static private String join(boolean[] values, String seperator) {
        boolean first=true;
        StringBuffer sb = new StringBuffer();
        for (Object v : values) {
            if( !first ) {
                sb.append(seperator);
            }
            first=false;
            sb.append(v);
        }
        return sb.toString();
    }
    
    static private String join(Object[] values, String seperator) {
        StringBuffer sb = new StringBuffer();
        boolean first=true;
        for (Object v : values) {
            if( !first ) {
                sb.append(seperator);
            }
            first=false;
            sb.append(v);
        }
        return sb.toString();
    }


    public static void main(String[] args) {
        String jv = System.getProperty("java.version").substring(0, 3);
        if (jv.compareTo("1.5") < 0) {
            System.err.println("This application requires jdk 1.5 or higher to run, the current java version is " + System.getProperty("java.version"));
            System.exit(-1);
            return;
        }

        IOBenchmark app = new IOBenchmark();
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
        
        IOBenchmark benchmark = new IOBenchmark();
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
        
        long lv;
        try {
            MemoryPropertyEditor mpe = new MemoryPropertyEditor();
            mpe.setAsText(blockSize);
            lv = (Long) mpe.getValue();
        } catch (Throwable e) {
            throw new UsageException("invalid block-size: "+blockSize);
        }
        blockSizeInBytes = (int) lv;
        
        try {
            MemoryPropertyEditor mpe = new MemoryPropertyEditor();
            mpe.setAsText(fileSize);
            lv = (Long) mpe.getValue();
        } catch (Throwable e) {
            throw new UsageException("invalid file-size: "+fileSize);
        }
        
        blocks = lv/blockSizeInBytes;
        if( (lv%blockSizeInBytes)>0 ) {
            blocks++;
        }
        
        
        PrintWriter pw = null;
        if( report!=null ) {
            pw = new PrintWriter(new FileOutputStream(report));
        }
        writeReportHeader(pw);
        int chartCounter=0;
        
        for (String operation : this.operation) {
            Combinator combinator = new Combinator();
            if( WRITE_OP.equals(operation) ) {
                combinator
                .put("preallocate", array(preallocate))
                .put("randomSeek", array(randomSeek))
                .put("sync", array(sync));
            } else if( READ_OP.equals(operation) ) {
                combinator.put("randomSeek", array(randomSeek));
                combinator.put("keepDirect", array(keepDirect));
            } else {
                throw new RuntimeException();
            }
            for (Map<String, Object> options : combinator.combinations()) {
                String title = "operation: "+operation+", "+options;
                System.out.println(title);
                System.out.println(repeat('-', title.length()));

                ArrayList<Benchmark> results = new ArrayList<Benchmark>(this.api.length); 
                for (String apiName : this.api) {
                    Benchmark benchmark = createBenchmark(apiName);
                    benchmark.file = new File(file);
                    benchmark.operation = operation;
                    benchmark.apiName = apiName;
                    benchmark.options = options;
                    IntrospectionSupport.setProperties(benchmark, options);
                    benchmark.execute();
                    results.add(benchmark);
                }
                writeReportChart(pw, chartCounter++, results);
            }
        }        
        
        writeReportFooter(pw);
        if( pw!=null) {
            pw.close();
            System.out.println("Benchmark report stored at: "+report);
        }

    }

    static private Object[] array(boolean[] value) {
        Object[] rc = new Object[value.length];
        for (int i = 0; i < rc.length; i++) {
            rc[i] = value[i];
        }
        return rc ;
    }


    private String repeat(char val, int length) {
        char [] rc = new char[length];
        Arrays.fill(rc, val);
        return new String(rc);
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
        pw.println("      body {font-family:Verdana; font-size:12px; color:#666666;}");
        pw.println("      * {margin:0; padding:0;}");
        pw.println("      .title {text-align:center;}");
        pw.println("      .chart-section {width:640px; padding: 10px; margin: 0px auto; clear: both;}");
        pw.println("      .chart-props {width:120px; padding:0; padding-top:5px; float:left; text-align:right; }");
        pw.println("      .chart-graph {width: 500px; height: 200px; float:right; }");
        pw.println("    </style>");
        pw.println("  </head>");
        pw.println("  <body>");
        pw.println("  <div class='title'> ");
        pw.println(format("    File Size: "+fileSize+", Block Size: "+blockSize+", Sample Period: %.2f, Samples: "+sampleCount, samplePeriod));
        pw.println("  </div>");
    }
    
    private void writeReportChart(PrintWriter pw, int id, ArrayList<Benchmark> data) {
        if(pw==null) 
            return;
        Benchmark d1 = data.get(0);
        
        String titleX = String.format("seconds", samplePeriod);
        String titleY = "operations / second";
        
        pw.println("    <div class='chart-section'>");
        pw.println("      <div class='chart-props'>");
        pw.print(  "        <div>operation: "+d1.operation+"</div>");
        for (Entry<String, Object> entry : new TreeMap<String, Object>(d1.options).entrySet()) {
          pw.print("<div>"+entry.getKey()+": "+entry.getValue()+"</div>");
        }
        pw.println();
        pw.println("      </div>");
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
            pw.print(String.format("          ['%.2f'", samplePeriod*(i+1)));
            for (Benchmark d : data) {
                double value = 0;
                if( d.samples.size() >i ) {
                    value = d.samples.get(i);
                }
                pw.print(String.format(", %.2f",value));
            }
            pw.print("]");
        }
        
        pw.println("        ]);");
        pw.println("        var chart = new google.visualization.LineChart(document.getElementById('chart_"+id+"'));");
        pw.println("        chart.draw(data, {legend: 'bottom', smoothLine:true, titleX:'"+titleX+"', titleY:'"+titleY+"' });");
        pw.println("      }");
        pw.println("    </script>");
        
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
        if( NATIVE_AIO_DIRECT_API.equals(api) ) {
            return new NativeDirectAioApi();
        }
        throw new RuntimeException("Unsupported API: "+api);
    }

    abstract class Benchmark {
        
        public Map<String, Object> options;
        public File file;
        public String apiName;
        public String operation;
        public boolean randomSeek; 
        public boolean sync;
        public boolean preallocate;
        public boolean keepDirect;
        public ArrayList<Double> samples;
        
        final public void execute() throws IOException {
            
            boolean write;
            if( WRITE_OP.equals(operation) ) {
                write = true;
            } else if( READ_OP.equals(operation) ) {
                write = false;
            } else {
                throw new RuntimeException();
            }
            
            System.out.println("  api: "+apiName);
            
            AtomicLong ioCount=new AtomicLong();
            RateSampler sampler = new RateSampler(ioCount, sampleCount, samplePeriod);
            sampler.verbose = verbose;
            try {
                
                Callback<byte[]> callback=null;
                if( write ) {
                    if( preallocate ) {
                        preallocate();
                    } else {
                        file.delete();
                    }
                } else {
                    if( !file.exists() ) {
                        preallocate();
                    } else {
                        if( file.length() != blocks*blockSizeInBytes ) {
                            file.delete();
                            preallocate();
                        }
                    }
                    if(keepDirect) {
                        callback = new Callback<byte[]>() {
                            public void onSuccess(byte[] result) {
                            }
                            public void onFailure(Throwable exception) {
                            }
                        };
                    }
                }
                
                Random rand=null;
                if( randomSeek ) {
                    rand = new Random((23<<16)/53);
                }
                

                init(write);
                if( verbose ) {
                    System.out.print("    sampling: ");
                }
                sampler.start();
                outer: while( true ) {
                    seek(0);
                    for( long i=0; i < blocks; i++) {
                        if( write ) {
                            if( keepDirect ) {
                                if( rand!=null ) {
                                    write(rand.nextLong()%blocks);
                                } else {
                                    write();
                                }
                            } else {
                                if( rand!=null ) {
                                    write(rand.nextLong()%blocks, block());
                                } else {
                                    write(block());
                                }
                            }
                            if( sync ) {
                                sync();
                            }
                        } else {
                            if( keepDirect ) {
                                if( rand!=null ) {
                                    read(rand.nextLong()%blocks, callback);
                                } else {
                                    read(callback);
                                }
                            } else {
                                if( rand!=null ) {
                                    read(rand.nextLong()%blocks);
                                } else {
                                    read();
                                }
                            }
                        }
                        ioCount.incrementAndGet();
                        if( !sampler.isAlive() ) {
                            break outer;
                        }
                    }
                }
            } finally {
                if( verbose ) {
                    System.out.println(" done");
                }
                dispose();
            }
            samples = sampler.getSamples();
            System.out.print("    samples (operations/second): ");
            boolean first=true;
            for (Double s : samples) {
                if( !first ) {
                    System.out.print(", ");
                }
                first=false;
                System.out.print(String.format("%.2f", s));
            }
            System.out.println();
            System.out.println();

        }

        protected void preallocate() throws IOException {
            // Pre-allocate the data file before we start sampling.
            if( verbose ) {
                System.out.println("    creating data file: ... ");
            }
            init(true);
            for( long i=0; i < blocks; i++) {
                write();
            }
            sync();
            dispose();
        }

        abstract protected void init(boolean write) throws IOException;
        abstract protected void dispose() throws IOException;
        abstract protected void seek(long pos) throws IOException;
        abstract protected void sync() throws IOException;
        
        abstract protected void write() throws IOException;
        protected void write(long pos) throws IOException {
            seek(pos);
            write();
        }
        
        abstract protected void write(byte []data) throws IOException;
        protected void write(long pos, byte []data) throws IOException {
            seek(pos);
            write(data);
        }
        
        abstract protected void read() throws IOException;
        abstract protected void read(Callback<byte[]> callback) throws IOException;
        protected void read(long pos) throws IOException {
            seek(pos);
            read();
        }
        protected void read(long pos, Callback<byte[]> callback) throws IOException {
            seek(pos);
            read(callback);
        }

    }
    
    
    final class NativeDirectAioApi extends Benchmark {
        
        private FileDescriptor fd;
        private NativeAllocation data;
        private byte[] block;
        private long offset;
        private FutureCallback<Long> callback;

        @Override
        protected void init(boolean write) throws IOException {
            block = block();
            data = NativeAllocation.allocate(block.length, false, 512);
            data.set(block);
            if( write ) {
                int oflags =  O_CREAT | O_TRUNC | O_WRONLY;
                int mode = S_IRUSR|S_IWUSR|S_IRGRP|S_IROTH;
                fd = FileDescriptor.open(file, oflags, mode);
            } else {
                int oflags =  O_RDONLY;
                fd = FileDescriptor.open(file, oflags);
            }
            fd.enableDirectIO();
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
            offset=pos;
        }

        @Override
        protected void sync() throws IOException {
        }

        private FutureCallback<Long> nextCallback(final Callback<byte[]> next) throws InterruptedIOException, IOException {
            if( callback!=null ) {
                try {
                    offset += callback.get();
                } catch (InterruptedException e) {
                    throw new InterruptedIOException();
                } catch (ExecutionException e) {
                    throw IOExceptionSupport.create(e.getCause());
                }
                callback=null;
            }
            callback = new FutureCallback<Long>() {
                @Override
                public void onSuccess(Long result) {
                    if( next != null ) {
                        data.get(block);
                        next.onSuccess(block);
                    }
                    super.onSuccess(result);
                }
            };
            return callback;
        }
        
        @Override
        protected void write() throws IOException {
            write(offset);
        }
        
        @Override
        protected void write(long offset) throws IOException {
            fd.write(offset, data, nextCallback(null));
        }

        @Override
        protected void read() throws IOException {
            read(offset);
        }
        
        protected void read(long offset) throws IOException {
            fd.read(offset, data, nextCallback(null));
        }

        @Override
        protected void read(long offset, Callback<byte[]> callback) throws IOException {
            fd.read(offset, data, nextCallback(callback));
        }
        
        @Override
        protected void read(Callback<byte[]> callback) throws IOException {
            read(offset, callback);
        }

        @Override
        protected void write(byte[] block) throws IOException {
            write(offset, block);
        }

        @Override
        protected void write(long offset, byte[] block) throws IOException {
            FutureCallback<Long> callback = nextCallback(null);
            data.set(block);
            fd.write(offset, data, callback);
        }

    }
    
    final class NativeBioApi extends Benchmark {
        
        private FileDescriptor fd;
        private NativeAllocation data;
        private byte[] block;

        @Override
        protected void init(boolean write) throws IOException {
            block = block();
            data = NativeAllocation.allocate(block.length, false, 512);
            data.set(block);
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
            long count = fd.read(data);
            if( count < data.length() ) {
                throw new IOException("Expecting a full read.");
            }
        }

        @Override
        protected void read(Callback<byte[]> callback) throws IOException {
            fd.read(data);
            data.get(block);
            callback.onSuccess(block);
        }

        @Override
        protected void write(byte[] block) throws IOException {
            data.set(block);
            fd.write(data);
        }

    }
    
    final class JavaApi extends Benchmark {
        
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

        @Override
        protected void read(Callback<byte[]> callback) throws IOException {
            raf.readFully(data);
            callback.onSuccess(data);
        }

        @Override
        protected void write(byte[] data) throws IOException {
            raf.write(data);
        }

    }
    
    Random DATA_RANDOM = new Random();
    private byte[] block() {
        byte []data = new byte[blockSizeInBytes];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte)('a'+(DATA_RANDOM.nextInt(26)));
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
    public void setBlockSize(String blockSize) {
        this.blockSize = blockSize;
    }
    public void setFileSize(String fileSize) {
        this.fileSize = fileSize;
    }
    public void setBlocks(long blocks) {
        this.blocks = blocks;
    }
    public void setFile(String file) {
        this.file = file;
    }
    public void setReport(String report) {
        this.report = report;
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
    public void setRandomSeek(boolean[] randomSeek) {
        this.randomSeek = randomSeek;
    }
    public void setKeepDirect(boolean[] keepDirect) {
        this.keepDirect = keepDirect;
    }
    public void setSync(boolean[] sync) {
        this.sync = sync;
    }
    public void setPreallocate(boolean[] preallocate) {
        this.preallocate = preallocate;
    }

}
