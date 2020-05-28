FROM cr.d.xiaomi.net/container/xiaomi_centos7:release
COPY --from=cr.d.xiaomi.net/hbase/openjdk-shenandoah:8 /opt/jdk /opt/jdk
ENV JAVA_HOME=/opt/jdk
ENV PATH $PATH:$JAVA_HOME/bin
RUN mkdir -p /home/work/app/hbase
RUN mkdir -p /home/work/packages/hbase
ADD hbase /home/work/packages/hbase
