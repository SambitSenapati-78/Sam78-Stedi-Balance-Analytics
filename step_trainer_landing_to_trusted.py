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

# Script generated for node Customer Curated
CustomerCurated_node1758708271823 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1758708271823")

# Script generated for node Step-Trainer Landing
StepTrainerLanding_node1758708317360 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1758708317360")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1758708472684 = ApplyMapping.apply(frame=StepTrainerLanding_node1758708317360, mappings=[("sensorreadingtime", "long", "right_sensorreadingtime", "long"), ("serialnumber", "string", "right_serialnumber", "string"), ("distancefromobject", "int", "right_distancefromobject", "int")], transformation_ctx="RenamedkeysforJoin_node1758708472684")

# Script generated for node Join
Join_node1758708574007 = Join.apply(frame1=CustomerCurated_node1758708271823, frame2=RenamedkeysforJoin_node1758708472684, keys1=["serialnumber"], keys2=["right_serialnumber"], transformation_ctx="Join_node1758708574007")

# Script generated for node Step-Trainer Trusted
EvaluateDataQuality().process_rows(frame=Join_node1758708574007, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1758708236594", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
StepTrainerTrusted_node1758708625573 = glueContext.getSink(path="s3://samproj78/step-trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1758708625573")
StepTrainerTrusted_node1758708625573.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1758708625573.setFormat("json")
StepTrainerTrusted_node1758708625573.writeFrame(Join_node1758708574007)
job.commit()
