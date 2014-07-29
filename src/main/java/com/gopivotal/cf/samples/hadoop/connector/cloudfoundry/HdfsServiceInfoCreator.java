package com.gopivotal.cf.samples.hadoop.connector.cloudfoundry;

import org.springframework.cloud.cloudfoundry.CloudFoundryServiceInfoCreator;
import org.springframework.cloud.cloudfoundry.Tags;

import java.util.Map;

public class HdfsServiceInfoCreator extends CloudFoundryServiceInfoCreator<HdfsServiceInfo> {

    public HdfsServiceInfoCreator() {
        super(new Tags("p-hd"));
    }

    @Override
    public HdfsServiceInfo createServiceInfo(Map<String, Object> serviceData) {
        try {
            Map<String, Object> credentials = (Map<String, Object>) serviceData.get("credentials");
            Map<String, Object> hdfs = (Map<String, Object>) credentials.get("hdfs");
            Map<String, Object> configuration = (Map<String, Object>) hdfs.get("configuration");
            // here's what we get in serviceData
            // {name=nfl-hadoop, label=p-hd, tags=[], plan=Standard, credentials={hadoop_username=uff849e1a5acd429, hdfs={configuration={fs.defaultFS=hdfs://192.168.109.60:8020}, directory=/user/uff849e1a5acd429}, yarn={configuration={yarn.resourcemanager.address=192.168.109.62:8032, mapreduce.framework.name=yarn, yarn.resourcemanager.scheduler.address=192.168.109.62:8030, mapreduce.job.working.dir=/user/uff849e1a5acd429/work, yarn.app.mapreduce.am.staging-dir=/user/uff849e1a5acd429/staging}}}}
            String id =  (String) serviceData.get("name");
            String hdfsUser = (String) credentials.get("hadoop_username");
            String hdfsDir = (String) hdfs.get("directory");
            String hdfsNamenode = (String) configuration.get("fs.defaultFS");
            return new HdfsServiceInfo(id, hdfsUser, hdfsDir, hdfsNamenode);
        } catch (Exception e) {
            return null;
        }
    }
    
    @Override
    public boolean accept(Map<String, Object> serviceData) {
        return createServiceInfo(serviceData) != null;
    }
}
