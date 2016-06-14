#¡/bin/bash
#echo "$(hostname) (old): running $(screen -ls | grep mkcargo | wc -l), .running ?, cargos $(find /home/root/wip/ -name "*.md5" | wc -l), completed $(find /home/root/wip/ -name "snapshot.csv" | wc -l)"
echo "$(hostname): running $(screen -ls | grep mkcargo | wc -l), .running $(find /home/root/wip/ -name ".running" | wc -l), cargos $(find /home/root/wip/ -name "*.md5" | wc -l), completed $(find /home/root/wip/ -name ".complete" | wc -l)"
ls /home/root/wip
