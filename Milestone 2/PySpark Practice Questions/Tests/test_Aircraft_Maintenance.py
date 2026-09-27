import os
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType,StructField,StringType,DoubleType
from datetime import date
import solution
ROOT=os.path.dirname(os.path.abspath(__file__)); LOG=os.path.join(ROOT,"test_report.log"); DATA=os.path.join(os.path.dirname(ROOT),"data","maintenance.csv")
def record(n,name,fn):
    try: fn(); line=f"Test Case {n:02d} : {name} : PASS"
    except Exception as e:
        line=f"Test Case {n:02d} : {name} : FAIL | Expected = required behavior | Reason = {e}"; open(LOG,"a",encoding="utf-8").write(line+"\n"); raise
    open(LOG,"a",encoding="utf-8").write(line+"\n")
def schema_fixture(): return StructType([StructField("maintenance_id",StringType(),True),StructField("aircraft_id",StringType(),True),StructField("maintenance_date",StringType(),True),StructField("maintenance_cost",DoubleType(),True),StructField("maintenance_status",StringType(),True)])
def test_01_define_maintenance_schema():
    def check():
        s=solution.define_maintenance_schema(); assert isinstance(s,StructType); assert [f.name for f in s.fields]==["maintenance_id","aircraft_id","maintenance_date","maintenance_cost","maintenance_status"]
    record(1,"define_maintenance_schema",check)
def test_02_load_maintenance_data(spark):
    def check():
        d=solution.load_maintenance_data(spark,DATA,schema_fixture()); assert isinstance(d,DataFrame); assert dict(d.dtypes)["maintenance_date"]=="date"; assert d.count()==7
    record(2,"load_maintenance_data",check)
def test_03_join_aircraft_with_maintenance(spark):
    def check():
        a=spark.createDataFrame([("AC1","A320","MAA","Active"),("AC2","B737","BOM","Active")],["aircraft_id","aircraft_model","base_airport","service_status"])
        m=spark.createDataFrame([("M1","AC1",date(2026,1,1),100.0,"Completed"),("M9","AC9",date(2026,1,1),200.0,"Completed")],["maintenance_id","aircraft_id","maintenance_date","maintenance_cost","maintenance_status"])
        out=solution.join_aircraft_with_maintenance(a,m); assert out.count()==1; assert out.collect()[0].aircraft_id=="AC1"
    record(3,"join_aircraft_with_maintenance",check)
def test_04_aircraft_without_maintenance(spark):
    def check():
        a=spark.createDataFrame([("AC1",),("AC2",),("AC3",)],["aircraft_id"]); m=spark.createDataFrame([("AC1",),("AC3",)],["aircraft_id"]); got=[r.aircraft_id for r in solution.aircraft_without_maintenance(a,m).collect()]; assert got==["AC2"]
    record(4,"aircraft_without_maintenance",check)
def test_05_rank_aircraft_by_maintenance_cost(spark):
    def check():
        d=spark.createDataFrame([("A320","AC1",100.0),("A320","AC1",50.0),("A320","AC2",200.0),("B737","AC3",300.0)],["aircraft_model","aircraft_id","maintenance_cost"]); got={(r.aircraft_model,r.aircraft_id):(r.total_maintenance_cost,r.maintenance_rank) for r in solution.rank_aircraft_by_maintenance_cost(d).collect()}; assert got[("A320","AC2")]==(200.0,1); assert got[("A320","AC1")]==(150.0,2); assert got[("B737","AC3")]==(300.0,1)
    record(5,"rank_aircraft_by_maintenance_cost",check)
def test_06_rank_tie_behavior(spark):
    def check():
        d=spark.createDataFrame([("A320","AC1",100.0),("A320","AC2",100.0),("A320","AC3",50.0)],["aircraft_model","aircraft_id","maintenance_cost"]); got={r.aircraft_id:r.maintenance_rank for r in solution.rank_aircraft_by_maintenance_cost(d).collect()}; assert got=={"AC1":1,"AC2":1,"AC3":3}
    record(6,"rank_aircraft_by_maintenance_cost_tie",check)
