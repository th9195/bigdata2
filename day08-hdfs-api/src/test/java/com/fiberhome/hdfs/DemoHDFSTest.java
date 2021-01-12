package com.fiberhome.hdfs;

import junit.framework.TestCase;
import org.junit.Before;
import org.junit.Test;

public class DemoHDFSTest extends TestCase {
    private static DemoHDFS demoHDFS = null;

    static {
        demoHDFS = new DemoHDFS();
    }

    @Test
    public void testGetFileSystem1() {
        demoHDFS.getFileSystem1();
    }

    @Test
    public void testGetFileSystem2() {
        demoHDFS.getFileSystem2();
    }

    @Test
    public void testListFiles() {
        demoHDFS.listFiles();
    }

    @Test
    public void testMkdirs() {
        demoHDFS.mkdirs();
    }

    @Test
    public void testDeleteDirs() {
        demoHDFS.deleteDirs();
    }

    @Test
    public void testPutFile() {
        demoHDFS.putFile();
    }

    @Test
    public void testDeleteFile() {
        demoHDFS.deleteFile();
    }

    @Test
    public void testExists_isFile() {
        demoHDFS.exists_isFile();
    }

    @Test
    public void testDownloadFile1() {
        demoHDFS.downloadFile1();
    }

    @Test
    public void testDownloadFile2() {
        demoHDFS.downloadFile2();
    }

    @Test
    public void testMergeFiles() {
        demoHDFS.mergeFiles();
    }

    @Test
    public void testMergeAppendFiles() {
        demoHDFS.mergeAppendFiles();
    }
}