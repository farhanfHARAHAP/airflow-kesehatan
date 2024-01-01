# airflow-kesehatan
Proses data .CSV untuk mendapatkan nilai COGS, COGAS, Net Sales, Gross Profit, dan Ending Inventory

## Requirement
- Windows 10
- WSL Ubuntu
- Apache Airflow
- Python 3.11
- Modul Pandas Python
- Mengunduh seluruh file di repository

## Cara Pemakaian

![image](https://github.com/farhanfHARAHAP/airflow-kesehatan/assets/91046795/b54d6c06-15b4-4815-a678-a101f2acb182)

Letakkan file di WSL Ubuntu anda.

![image](https://github.com/farhanfHARAHAP/airflow-kesehatan/assets/91046795/31e37c09-cef5-49e7-87c2-eb8c65728a6f)

Buka direktori \\wsl$\Ubuntu\home\(user)\airflow

![image](https://github.com/farhanfHARAHAP/airflow-kesehatan/assets/91046795/dd378bad-7c50-48fd-bad5-932432814fcb)

Buka file airflow.cfg

![image](https://github.com/farhanfHARAHAP/airflow-kesehatan/assets/91046795/c95c9698-88fd-4d02-aef3-7e550599eb4f)

Paste code ini di bawah line 6 atau 7.
dags_folder = /home/(user)/bigdata/tutorial_dags

![image](https://github.com/farhanfHARAHAP/airflow-kesehatan/assets/91046795/3701ed7e-fb41-4066-98c6-95dc3dc915b8)

Buka dua window WSL ubuntu dan masuk ke venv yang telah terinstal airflow.

![image](https://github.com/farhanfHARAHAP/airflow-kesehatan/assets/91046795/c080ca24-4134-4a33-b53b-f0c66eba0422)

Ketik command ini di WSL ubuntu:
Window 1: airflow scheduler
window 2: airflow webserver

![image](https://github.com/farhanfHARAHAP/airflow-kesehatan/assets/91046795/210ba12e-34a3-4810-8bcf-80b8531c86b3)

Masuk ke URL dibawah dengan browser anda:
localhost:8080

![image](https://github.com/farhanfHARAHAP/airflow-kesehatan/assets/91046795/670d471b-a291-4b36-9877-3d64d4356cc0)

Cari DAG sehat_dag

![image](https://github.com/farhanfHARAHAP/airflow-kesehatan/assets/91046795/1e2b7706-cbfb-46a5-adf9-082765cd4b61)

Buka DAG dan run DAG!

![image](https://github.com/farhanfHARAHAP/airflow-kesehatan/assets/91046795/5f6e4ff1-fe92-4af7-950e-1872a70eddb6)

Setelah run berhasil, buka direktori dibawah untuk melihat hasil proses data
\\wsl$\Ubuntu\home\(user)\bigdata\tutorial_dags\sehat_data\statement

