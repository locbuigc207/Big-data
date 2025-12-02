✅ Chuẩn bị môi trường

Setup Kubernetes cluster (nếu chưa có):

Sử dụng Minikube local: minikube start --cpus=4 --memory=8192
Hoặc cloud: GKE, EKS, AKS


Deploy HDFS:

bash   kubectl apply -f hdfs-complete-deployment.yaml
   kubectl get pods -n bigdata -w

Deploy Kafka (với Strimzi):

bash   kubectl create -f 'https://strimzi.io/install/latest?namespace=bigdata'
   kubectl apply -f kafka-deployment.yaml

Deploy databases (MongoDB, PostgreSQL):

Đã bao gồm trong hdfs-complete-deployment.yaml


Verify services:

bash   kubectl get all -n bigdata
   kubectl port-forward svc/hdfs-namenode -n bigdata 30870:9870
   # Open http://localhost:30870
✅ Triển khai Batch Ingestion

Tạo data files (nếu chưa có):

bash   cd generate_fake_data
   jupyter notebook generate_fake_data.ipynb
   # Run all cells để tạo CSV files

Upload CSV files lên K8s:

bash   chmod +x upload-csv-data.sh
   ./upload-csv-data.sh ./generate_fake_data

Deploy Python script:

bash   kubectl create configmap batch-scripts \
     --from-file=batch_ingestion_k8s.py \
     -n bigdata

Deploy batch job:

bash   kubectl apply -f batch-ingestion-job.yaml

Run manual ingestion (test):

bash   kubectl create job --from=cronjob/batch-ingestion-daily \
     batch-test-$(date +%s) -n bigdata

Monitor job:

bash   kubectl get jobs -n bigdata
   kubectl logs -f job/batch-ingestion-manual -n bigdata
✅ Verification Steps

Check HDFS data:

bash   kubectl exec -it hdfs-namenode-0 -n bigdata -- bash
   hdfs dfs -ls /raw
   hdfs dfs -ls /raw/students/$(date +%Y-%m-%d)
   hdfs dfs -cat /raw/students/$(date +%Y-%m-%d)/part-00000*.parquet | head

Check quality metrics:

bash   hdfs dfs -ls /quality_metrics
   hdfs dfs -cat /quality_metrics/students/$(date +%Y-%m-%d)/*.json


Verify record counts:
   - Students: ~3,000 records
   - Enrollments: ~20,000 records
   - Grades: ~20,000 records
   - Attendance: ~600,000 records