/*для полных админских прав*/

sudo /usr/sbin/repquota -gs /sas_tmp_work
sudo /usr/sbin/repquota -gs /SAS_CS

du -scBM /SAS_CS/URD/* | sort -nr
du -scBM /sas_tmp_work/* | sort -nr
 du -scBM /sas_tmp_work/* user  | grep /sas_tmp_work/

find /SAS_CS/* -size +100k -exec ls -lkq --time-style=long-iso '{}' \; | awk '{print $3" "$4" "$5" "$6" "$8}' > /home/sas/1.txtfind /SAS_CS/* -size +100k -exec ls -lkq --time-style=long-iso '{}' \; | awk '{print $3" "$4" "$5" "$6" "$8}' > /home/sas/1.txt


/*-------Для пользователя Trakhachev_V*/
ps -aeo user:15,pid,ppid,stime,time,cmd | grep /SAS_CS/ 
ps -aeo user:15,pid,ppid,stime,time,cmd | grep /SAS_CS/ | grep Trakhachev_V
ps -aeo user:15,pid,ppid,stime,time,cmd | grep /SAS_CS/ | grep Lyalin_N

find /SAS_CS/* -size +100k -exec ls -lkq --time-style=long-iso '{}' \; | awk '{print $3" "$4" "$5" "$6" "$8}' > /home/sas/1.txt

kill 32016
