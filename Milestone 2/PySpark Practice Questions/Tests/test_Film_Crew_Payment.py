import os
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType,StructField,StringType,DoubleType
from datetime import date
import solution
ROOT=os.path.dirname(os.path.abspath(__file__)); LOG=os.path.join(ROOT,"test_report.log"); DATA=os.path.join(os.path.dirname(ROOT),"data","payments.csv")
def record(n,name,fn):
    try: fn(); line=f"Test Case {n:02d} : {name} : PASS"
    except Exception as e:
        line=f"Test Case {n:02d} : {name} : FAIL | Expected = required behavior | Reason = {e}"; open(LOG,"a",encoding="utf-8").write(line+"\n"); raise
    open(LOG,"a",encoding="utf-8").write(line+"\n")
def schema_fixture(): return StructType([StructField("payment_id",StringType(),True),StructField("crew_id",StringType(),True),StructField("payment_date",StringType(),True),StructField("payment_amount",DoubleType(),True),StructField("payment_status",StringType(),True)])
def test_01_define_payment_schema():
    def check():
        s=solution.define_payment_schema(); assert [f.name for f in s.fields]==["payment_id","crew_id","payment_date","payment_amount","payment_status"]
    record(1,"define_payment_schema",check)
def test_02_load_and_clean_payments(spark):
    def check():
        d=solution.load_and_clean_payments(spark,DATA,schema_fixture()); assert isinstance(d,DataFrame); assert d.count()==8; assert dict(d.dtypes)["payment_date"]=="date"; assert "payment_year" in d.columns and "payment_month" in d.columns; assert d.filter("payment_status = 'Pending'").count()==1
    record(2,"load_and_clean_payments",check)
def test_03_join_crew_with_payments(spark):
    def check():
        c=spark.createDataFrame([("C1","A","Camera","X"),("C2","B","Sound","Y")],["crew_id","crew_name","department","city"]); p=spark.createDataFrame([("P1","C1",100.0),("P9","C9",500.0)],["payment_id","crew_id","payment_amount"]); out=solution.join_crew_with_payments(c,p); assert out.count()==1 and out.collect()[0].crew_id=="C1"
    record(3,"join_crew_with_payments",check)
def test_04_crew_without_payments(spark):
    def check():
        c=spark.createDataFrame([("C1",),("C2",),("C3",)],["crew_id"]); p=spark.createDataFrame([("C1",),("C3",)],["crew_id"]); assert [r.crew_id for r in solution.crew_without_payments(c,p).collect()]==["C2"]
    record(4,"crew_without_payments",check)
def test_05_rank_crew_by_total_payment(spark):
    def check():
        d=spark.createDataFrame([("Camera","C1","A",100.0),("Camera","C1","A",50.0),("Camera","C2","B",200.0),("Sound","C3","C",90.0)],["department","crew_id","crew_name","payment_amount"]); got={r.crew_id:(r.total_payment,r.payment_rank) for r in solution.rank_crew_by_total_payment(d).collect()}; assert got["C2"]==(200.0,1) and got["C1"]==(150.0,2) and got["C3"]==(90.0,1)
    record(5,"rank_crew_by_total_payment",check)
def test_06_rank_crew_tie(spark):
    def check():
        d=spark.createDataFrame([("Camera","C1","A",100.0),("Camera","C2","B",100.0),("Camera","C3","C",50.0)],["department","crew_id","crew_name","payment_amount"]); got={r.crew_id:r.payment_rank for r in solution.rank_crew_by_total_payment(d).collect()}; assert got=={"C1":1,"C2":1,"C3":3}
    record(6,"rank_crew_by_total_payment_tie",check)
