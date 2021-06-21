# ports
iptables -A INPUT -p tcp --dport 8989 -j ACCEPT
iptables -A INPUT -p tcp --dport 5000 -j ACCEPT
systemctl start firewalld
firewall-cmd --zone=public --permanent --add-port 8989/tcp
firewall-cmd --zone=public --permanent --add-port 5000/tcp
firewall-cmd --reload

# docker
yum install -y yum-utils
yum-config-manager \
    --add-repo \
    https://download.docker.com/linux/centos/docker-ce.repo

yum install docker-ce docker-ce-cli containerd.io
systemctl start docker

# osrm engine
wget http://download.geofabrik.de/europe/ukraine-latest.osm.pbf
docker run -t -v "${PWD}:/data" osrm/osrm-backend osrm-extract -p /opt/car.lua /data/ukraine-latest.osm.pbf
docker run -t -v "${PWD}:/data" osrm/osrm-backend osrm-partition /data/ukraine-latest.osrm
docker run -t -v "${PWD}:/data" osrm/osrm-backend osrm-customize /data/ukraine-latest.osrm
docker run -t -i -p -d 5000:5000 -v "${PWD}:/data" osrm/osrm-backend osrm-routed --algorithm mld /data/ukraine-latest.osrm

# graphhopper
yum install java-11-openjdk-devel
yum install java-11-openjdk
export JAVA_OPTS="-Xmx2g -Xms2g"
git clone git://github.com/graphhopper/graphhopper.git
cd graphhopper
git checkout master
./graphhopper.sh -a web -i europe_ukraine.pbf -o ukraine-gh --port 8989