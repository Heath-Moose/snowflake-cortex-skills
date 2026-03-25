---
name: partitioned-inference
description: Partitioned inference with CustomModel and @partitioned_api decorator
path: machine-learning/model-registry/partitioned-inference
parent_skill: model-registry
---

# Partitioned Inference

Parallelize inference across data partitions using the Model Registry. Train and predict on partition-specific submodels.

> **Execution Environment**: Partitioned inference runs on **virtual warehouse** (not compute pool). For compute pool-based distributed processing, see `../../distributed-training/SKILL.md`.

## When to Use
- Dataset has natural partitions (store, region, customer segment)
- Partitions are independent (uncorrelated data)
- Each partition has sufficient data for training
- Need to parallelize training/inference

## Stateless Partitioned Model

Training and inference happen together; no stored fit state.

```python
import pandas as pd
from snowflake.ml.model.custom_model import CustomModel, partitioned_api

class StatelessPartitionedModel(CustomModel):
    
    @partitioned_api
    def predict(self, input_df: pd.DataFrame) -> pd.DataFrame:
        import xgboost
        
        # Train on partition data
        X = input_df[['feature1', 'feature2']]
        y = input_df['target']
        
        model = xgboost.XGBRegressor()
        model.fit(X, y)
        
        # Generate predictions
        predictions = model.predict(X)
        return pd.DataFrame({'prediction': predictions})

my_model = StatelessPartitionedModel()
```

## Stateful Partitioned Model

Pre-trained submodels loaded from context. **Two-phase workflow**: train models per partition first, then package for inference.

> **Training models per partition**: If you need to train models first, load `../../distributed-training/mmt/SKILL.md`. This skill covers only the inference side — packaging and running predictions with pre-trained models.

### Step 1: Package Pre-Trained Models into ModelContext

After training models per partition (via MMT or any other method), create a `ModelContext`. There are two options depending on model size:

**Option A: In-memory models** (default — models loaded into memory):

```python
from snowflake.ml.model.custom_model import CustomModel, ModelContext, partitioned_api

# models dict maps partition_id -> fitted model object
models = {
    "store_1": fitted_model_1,
    "store_2": fitted_model_2,
}

model_context = ModelContext(models=models)
stateful_model = StatefulPartitionedModel(context=model_context)  # Note: parameter is 'context', not 'model_context'
```

**Option B: File artifacts** (for large models that are expensive to hold in memory):

```python
model_context = ModelContext(
    artifacts={
        "store_1": "/path/to/model1.pkl",
        "store_2": "/path/to/model2.pkl",
    }
)
```

### Step 2: Define the CustomModel with Partition Lookup

The `predict` method extracts the partition ID from input data and retrieves the corresponding pre-trained submodel.

**For in-memory models (Option A)** — use `self.context.model_ref()`:

```python
class StatefulPartitionedModel(CustomModel):
    
    @partitioned_api
    def predict(self, input: pd.DataFrame) -> pd.DataFrame:
        partition_id = input["STORE_NUMBER"][0]
        model = self.context.model_ref(partition_id)
        
        predictions = model.predict(input[['feature1', 'feature2']])
        return pd.DataFrame({'prediction': predictions})
```

**For file artifacts (Option B)** — use `self.context.path()` to load on demand:

```python
class ArtifactPartitionedModel(CustomModel):
    
    @partitioned_api
    def predict(self, input: pd.DataFrame) -> pd.DataFrame:
        partition_id = input["STORE_NUMBER"][0]
        model_path = self.context.path(partition_id)
        
        import joblib
        model = joblib.load(model_path)
        
        predictions = model.predict(input[['feature1', 'feature2']])
        return pd.DataFrame({'prediction': predictions})
```

## Logging Partitioned Models

### Automatic Dependency Inference

Snowflake automatically infers dependencies during `log_model()` when you provide `sample_input_data`. This means `conda_dependencies` is usually not needed — frameworks like xgboost, sklearn, lightgbm are detected automatically.

```python
from snowflake.ml.registry import Registry
reg = Registry(session=session, database_name="ML", schema_name="REGISTRY")

# No conda_dependencies needed
model_version = reg.log_model(
    my_model,
    model_name="my_partitioned_model",
    version_name="v1",
    options={"function_type": "TABLE_FUNCTION"},
    sample_input_data=train_features,  # Required for dependency inference
)
```

### Manual Dependencies (Fallback)

If automatic inference fails or you need specific versions, pin `conda_dependencies`:

```python
model_version = reg.log_model(
    my_model,
    model_name="my_partitioned_model",
    version_name="v1",
    options={"function_type": "TABLE_FUNCTION"},
    sample_input_data=train_features,
    conda_dependencies=["xgboost==1.7.6", "cloudpickle==2.2.1"],
)
```

> **Serialization errors?** Pin `conda_dependencies` to match the versions used during training. Common symptoms:
> - `AttributeError: Can't get attribute '_class_setstate'` → cloudpickle version mismatch (try `cloudpickle==2.2.1`)
> - `ModuleNotFoundError: No module named 'numpy._core.numeric'` → numpy 2.x incompatibility (try `numpy<2`)


## Running Partitioned Inference

### Python API

Call the model version with a partition column to distribute inference across partitions:

```python
model_version.run(
    input_df,
    function_name="PREDICT",
    partition_column="STORE_NUMBER"
)
```

### SQL

Equivalent SQL using `PARTITION BY` to route rows to the correct submodel:

```sql
SELECT output1, output2, partition_column
FROM input_table, 
    TABLE(
        my_model!predict(input_table.input1, input_table.input2)
        OVER (PARTITION BY input_table.store_number)
    )
ORDER BY input_table.store_number;
```

## Key Differences: Partitioned Models vs Many Model Training

| Aspect | Partitioned Inference | Many Model Training (MMT) |
|--------|----------------------|---------------------------|
| Primary use | Inference parallelization | Training parallelization |
| Storage | Model Registry | Snowflake Stage |
| Decorator | `@partitioned_api` | N/A |
| Output | Predictions per partition | Trained models per partition |
| Framework | CustomModel subclass | Training function |

**How they work together (Stateful Workflow):**
```
MMT (train per partition) → get_model() → ModelContext → Partitioned Inference
```

1. **MMT trains** models per partition → outputs to stage
2. **get_model()** retrieves fitted models
3. **ModelContext** packages models for CustomModel
4. **Partitioned Inference** runs predictions using pre-fitted models

## Key Classes

| Class/Decorator | Import |
|-----------------|--------|
| `CustomModel` | `snowflake.ml.model.custom_model` |
| `@partitioned_api` | `snowflake.ml.model.custom_model` |
| `ModelContext` | `snowflake.ml.model.custom_model` |
| `Registry` | `snowflake.ml.registry` |

## Quickstarts
- [Partitioned Model Quickstart](https://quickstarts.snowflake.com/guide/partitioned-ml-model/)
- [Many Model Inference Quickstart](https://quickstarts.snowflake.com/guide/many-model-inference-in-snowflake/)
