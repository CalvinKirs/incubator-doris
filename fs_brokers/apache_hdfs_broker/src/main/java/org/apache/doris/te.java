package org.apache.doris;

import org.apache.doris.common.WildcardURI;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;

import java.io.IOException;

public class te {

    public static void main(String[] args) throws IOException {
        String path = "gs://doris-test/te.txt";
        WildcardURI pathUri = new WildcardURI(path);
        Configuration conf = new Configuration();
        conf.set("fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem");
        conf.set("fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS");
        conf.set("fs.gs.project.id", "doris-test");
        conf.set("fs.gs.working.dir", "/");
        conf.set("fs.gs.auth.service.account.enable", "true");
        conf.set("fs.gs.auth.service.account.json.keyfile", "/Users/zhengxiaoqiang/Downloads/doris-test-0f3c7c3e9e6a.json");
        FileSystem fs = FileSystem.newInstance(pathUri.getUri(), conf);
    }
}
