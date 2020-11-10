package edu.ucr.cs.cs226.ppoth001;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * Hello world!
 *
 */
public class HDFSUpload
{
    public static void main( String[] args ) throws IOException
    {
        Path srcPath = null;
        Path destPath = null;
        if(args.length==2){
            srcPath = new Path(args[0]);
            destPath = new Path(args[1]);
        } else {
            System.out.println("Source path and Destination path are required");
            return;
        }
        Configuration conf = new Configuration();

        FileSystem srcFS = srcPath.getFileSystem(conf);
        FileSystem destFS = destPath.getFileSystem(conf);

        if (!srcFS.exists(srcPath)) {
            System.err.println("Source file does not exist");
            srcFS.close();
            return;
        }
        if (destFS.exists(destPath)) {
            System.out.println("Target file already exists");
            srcFS.close();
            destFS.close();
            return;
        }

        FSDataInputStream inputStream = srcFS.open(srcPath);
        FSDataOutputStream outputStream = destFS.create(destPath);
        byte streamBuffer[] = new byte[256];
        try {
            int bytesRead = 0;
            long startTime = System.currentTimeMillis();
            while ((bytesRead = inputStream.read(streamBuffer)) > 0) {
                outputStream.write(streamBuffer, 0, bytesRead);
            }
            long endTime = System.currentTimeMillis();
            System.out.println("Time taken to copy from source path to destination path in milliseconds: " + (endTime - startTime));

        } catch (IOException e) {
            System.out.println("Error while copying file");
            e.printStackTrace();
        } finally {
            inputStream.close();
            outputStream.close();
            srcFS.close();
            destFS.close();
        }
    }
}