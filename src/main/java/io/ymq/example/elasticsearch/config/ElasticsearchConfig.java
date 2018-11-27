package io.ymq.example.elasticsearch.config;

import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.TransportAddress;
import org.elasticsearch.transport.client.PreBuiltTransportClient;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.InetAddress;

/**
 * 描述: elasticsearch 配置
 *
 * @author yanpenglei
 * @create 2017-11-02 16:41
 **/
@Configuration
public class ElasticsearchConfig {

    //private static final Logger LOGGER = LoggerFactory.getLogger(ElasticsearchConfig.class);

    /**
     * elk集群地址
     */
    @Value("${elasticsearch.ip}")
    private String hostName;
    /**
     * 端口
     */
    @Value("${elasticsearch.port}")
    private String port;
    /**
     * 集群名称
     */
    @Value("${elasticsearch.cluster.name}")
    private String clusterName;

    /**
     * 连接池
     */
    @Value("${elasticsearch.pool}")
    private String poolSize;

    @Bean
    public TransportClient init() {

        TransportClient transportClient = null;

        try {
            /* 配置信息
            Settings esSetting = Settings.builder()
                    //.put("cluster.name", clusterName)
                   // .put("client.transport.sniff", false)//增加嗅探机制，找到ES集群
                  //  .put("thread_pool.search.size", Integer.parseInt(poolSize))//增加线程池个数，暂时设为5
                    .build();

            transportClient = new PreBuiltTransportClient(esSetting).
                    addTransportAddresses(new TransportAddress(InetAddress.getByName(hostName), Integer.parseInt(port)));
*/
            // 设置集群名称
            Settings settings = Settings.builder().put("cluster.name", clusterName).build();
            // 创建client
            transportClient = new PreBuiltTransportClient(settings)
                    .addTransportAddress(new TransportAddress(InetAddress.getByName(hostName), Integer.parseInt(port)));

            /*
            //创建client
            transportClient = new PreBuiltTransportClient(esSetting)
                    .addTransportAddress(new InetSocketTransportAddress(InetAddress.getByName(hostName), 9300));
            */


        } catch (Exception e) {
            System.out.println("elasticsearch TransportClient create error!!!" + e.getMessage());
        }

        return transportClient;
    }


}
