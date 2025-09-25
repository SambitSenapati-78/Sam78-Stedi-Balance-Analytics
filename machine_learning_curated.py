import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1758709115130 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1758709115130")

# Script generated for node Step-Trainer Trusted
StepTrainerTrusted_node1758709061133 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1758709061133")

# Script generated for node Join
Join_node1758709146817 = Join.apply(frame1=StepTrainerTrusted_node1758709061133, frame2=AccelerometerTrusted_node1758709115130, keys1=["right_sensorreadingtime"], keys2=["timestamp"], transformation_ctx="Join_node1758709146817")

# Script generated for node Drop Fields
DropFields_node1758709884878 = DropFields.apply(frame=Join_node1758709146817, paths=["registrationdate", "customername", "birthday", "sharewithfriendsasofdate", "sharewithpublicasofdate", "serialnumber", "phone", "sharewithresearchasofdate", "lastupdatedate"], transformation_ctx="DropFields_node1758709884878")

# Script generated for node Step-Trainer Curated
EvaluateDataQuality().process_rows(frame=DropFields_node1758709884878, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758708998296", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerCurated_node1758710409401 = glueContext.getSink(path="s3://samproj78/step-trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerCurated_node1758710409401")
StepTrainerCurated_node1758710409401.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_curated")
StepTrainerCurated_node1758710409401.setFormat("json")
StepTrainerCurated_node1758710409401.writeFrame(DropFields_node1758709884878)
job.commit()
