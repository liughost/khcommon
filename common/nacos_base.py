import hashlib
import json5
import re
import threading
import time
import nacos
import logging
import socket

class NacosBase:
    def __init__(
        self,
        host,
        namespace,
        service_name,
        # ip="127.0.0.1",
        port="8000",
        group_name="kh-medical",
        cluster_name="DEFAULT",
        heart_beat=5,
        verbose=1
    ) -> None:
        self.conf_md5 = None
        self.conf = None
        self.group_name = group_name
        self.namespace = namespace
        self.cluster_name = cluster_name
        self.service_name = service_name
        self.ip = '127.0.0.1'
        self.port = port
        self.verbose = verbose
        self.client = nacos.NacosClient(host, namespace=namespace)
        nacos.client.logger.setLevel(level=logging.WARN)
        threading.Timer(
            heart_beat, self.nacos_service_register, args=(heart_beat,)
        ).start()
        
    def get_register_ip(self,ip_pre):
        host_name = socket.gethostname()
        ip_eths = socket.gethostbyname_ex(host_name)
        logging.info(f"ip_eths={ip_eths},ip_pre={ip_pre}")
        for ips in ip_eths:
            for ip in ips:
                if ip_pre in ip:
                    return ip # 要求网段的ip
        return '127.0.0.1'
    def read(self):
        return self.conf
    
    def list_instance_url(self,service_name):
        """
        查询指定服务请求地址和端口
        """
        service = self.client.list_naming_instance(service_name, self.cluster_name,self.namespace,self.group_name,True)
        if service is None or 'hosts' not in service:
            return None
        hosts = service['hosts']
        for host in hosts:
            yield f"http://{host['ip']}:{host['port']}/"

    def nacos_service_register(self, ts):
        if self.verbose>1:
            logging.debug(f"nacos_service_register,ts={ts}")
        try:
            res = self.client.add_naming_instance(
                f"{self.group_name}@@{self.service_name}",
                self.ip,
                self.port,
                cluster_name=self.cluster_name,
                group_name=self.group_name,
                weight=1,
                ephemeral=True,
                enable=True,
            )
            # logging.debug(f"{res}")
            if res and self.conf is None:
                self.conf = self.client.get_config(
                    self.service_name, group=self.group_name
                )
                self.nacos_config(self.conf)

        except Exception as e:
            logging.info(f"ip={self.ip},port={self.port}")
            logging.error(e)
        threading.Timer(ts, self.nacos_service_register, args=(ts,)).start()
    def config_md5(self, conf_text):
        """计算配置的md5"""
        md = hashlib.md5()     #获取一个md5加密算法对象
        md.update(conf_text.encode('utf-8'))
        return md.hexdigest()

    def nacos_config(self, conf_text):
        """读取nacos的参数"""
        # 计算新的md5
        conf_md5 = self.config_md5(conf_text)
        if self.conf_md5 is not None and self.conf_md5 == conf_md5:
            print("配置没有变化")
            return
        # 更新md5
        self.conf_md5 = conf_md5
        logging.info(f"conf_md5={conf_md5},conf_text:\n{conf_text}")
        # 处理注释
        self.conf = json5.loads(conf_text)
        if 'nacos_preferred_network' in self.conf:
            self.ip = self.get_register_ip(self.conf['nacos_preferred_network'])
            logging.info(f"nacos_preferred_network={self.conf['nacos_preferred_network']},ip={self.ip}")

    def get_service_list(self, service_name):
        """获取服务列表"""
        return self.client.list_naming_instance(
            service_name,
            clusters=self.cluster_name,
            namespace_id=self.namespace,
            group_name=self.group_name,
            healthy_only=True,
        )
    
    def get_service_host(self, service_name):
        """获取服务列表"""
        service = self.client.list_naming_instance(
            service_name,
            clusters=self.cluster_name,
            namespace_id=self.namespace,
            group_name=self.group_name,
            healthy_only=True,
        )
        if 'hosts' not in service:
            return None,None
        return filter(lambda x:'127.0.0' not in x['ip'],service['hosts'])

if __name__ == "__main__":
    n = NacosBase(
        "http://172.16.1.159:8848",
        "d3c3f66e-d800-47f1-9e7e-c764a2ef7a99",
        "kh-medical-core-service-recommend-admin",
    )
    api_list = n.list_instance_url("kh-medical-core-service-smartdevice-api")
    print(list(api_list))
    while True:
        time.sleep(10)
        print(".", end="")
        service_list = n.get_service_host("kh-medical-core-service-recommend-admin")
        print(service_list)
        api_list = n.list_instance_url("kh-medical-core-service-smartdevice-api")
        print(list(api_list))
