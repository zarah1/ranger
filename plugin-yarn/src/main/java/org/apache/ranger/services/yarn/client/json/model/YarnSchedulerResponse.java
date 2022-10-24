package org.apache.ranger.services.yarn.client.json.model;

import org.codehaus.jackson.annotate.JsonAutoDetect;
import org.codehaus.jackson.annotate.JsonAutoDetect.Visibility;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;
import org.codehaus.jackson.map.annotate.JsonSerialize;

import javax.xml.bind.annotation.XmlAccessType;
import javax.xml.bind.annotation.XmlAccessorType;
import javax.xml.bind.annotation.XmlRootElement;
import java.util.ArrayList;
import java.util.List;


@JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
@JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL )
@JsonIgnoreProperties(ignoreUnknown=true)
@XmlRootElement
@XmlAccessorType(XmlAccessType.FIELD)
public class YarnSchedulerResponse {

    private static final long serialVersionUID = 1L;

    private YarnScheduler scheduler = null;

    public YarnScheduler getScheduler() { return scheduler; }

    public List<String> getQueueNames(String yarnVersion) {
        List<String> ret = new ArrayList<String>();

        if(scheduler != null) {
            scheduler.collectQueueNames(ret, yarnVersion);
        }

        return ret;
    }


    @JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL )
    @JsonIgnoreProperties(ignoreUnknown=true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class YarnScheduler implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private YarnSchedulerInfo schedulerInfo = null;

        public YarnSchedulerInfo getSchedulerInfo() { return schedulerInfo; }

        public void collectQueueNames(List<String> queueNames, String yarnVersion) {
            if(schedulerInfo != null) {
                if (yarnVersion.equals("hdp")) {
                    schedulerInfo.collectQueueNames(queueNames, null);
                } else {
                    schedulerInfo.collectQueueNames(queueNames);
                }
            }
        }
    }

    @JsonAutoDetect(getterVisibility=Visibility.NONE, setterVisibility=Visibility.NONE, fieldVisibility=Visibility.ANY)
    @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL )
    @JsonIgnoreProperties(ignoreUnknown=true)
    @XmlRootElement
    @XmlAccessorType(XmlAccessType.FIELD)
    public static class YarnSchedulerInfo implements java.io.Serializable {
        private static final long serialVersionUID = 1L;

        private String     queueName = null;

        private YarnCDHSchedulerResponse.YarnRootQueue rootQueue = null;

        private YarnHDPSchedulerResponse.YarnQueues queues    = null;

        public String getQueueName() { return queueName; }

        public YarnCDHSchedulerResponse.YarnRootQueue getRootQueue() { return rootQueue; }

        public YarnHDPSchedulerResponse.YarnQueues getQueues() { return queues; }


        public void collectQueueNames(List<String> queueNames) {
            if(rootQueue != null) {
                rootQueue.collectQueueNames(queueNames);
            }
        }

        public void collectQueueNames(List<String> queueNames, String parentQueueName) {
            if(queueName != null) {
                String queueFqdn = parentQueueName == null ? queueName : parentQueueName + "." + queueName;

                queueNames.add(queueFqdn);

                if(queues != null) {
                    queues.collectQueueNames(queueNames, queueFqdn);
                }
            }
        }
    }
}
