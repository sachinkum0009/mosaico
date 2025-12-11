# Core Concepts

This guide provides a high-level overview of how the platform structures, stores, and manages data. 
To effectively use Mosaico, it is essential to understand the three pillars of its architecture: **Ontology**, **Topic**, and **Sequence**. 
These concepts work together to transform raw data into a structured, semantic representation that represents the foundation of the platform's processing capabilities.

## The Ontology

The Ontology is the structural backbone of Mosaico. 
It serves as a semantic representation of all data used within your application, whether that consists of simple sensor readings or the complex results of an algorithmic process.

In Mosaico, all data is viewed through the lens of **time series**. 
Even a single data point is treated as a singular case of a time series. 
The ontology defines the "shape" of this data. It can represent base types (such as integers, floats, or strings) as well as complex structures (such as specific sensor arrays or processing results).

This abstraction allows Mosaico to understand what your data *is*, rather than just storing it as raw bytes. 
By using an ontology to inject and index data, you enable the platform to perform ad-hoc processing, such as custom compression or semantic indexing, tailored specifically to the type of data you have ingested.

Users can easily extend the platform by defining their own **Ontology Models**. These are specific data structures representing a single data type. For example, a GPS sensor might be modeled as follows:

```python
class GPS:
    latitude: Float
    longitude: Float
    altitude: Float
    # ... additional fields
```

## Topics and Sequences

Once you have an Ontology Model, you need a way to instantiate it and store actual data. This is where the **Topic** comes in. A Topic is a concrete instance of a specific Ontology Model. It functions as a container for a particular time series holding that specific data model. There is a strict one-to-one relationship here: one Topic corresponds to exactly one Ontology Model. This relationship allows you to query specific topics within the platform based on their semantic structure.

However, data rarely exists in isolation. Topics are usually part of a larger context. In Mosaico, this context is provided by the **Sequence**. A Sequence is a collection of logically related Topics.

To visualize this, think of a **ROS bag** or a recording of a robot's run. The recording session itself is the Sequence. Inside that Sequence, you have readings from a Lidar sensor, a GPS unit, and an accelerometer. Each of those individual sensor streams is a Topic, and each Topic follows the structure defined by its Ontology Model. Both Topics and Sequences can hold metadata to further describe their contents.

## Data Lifetime and Integrity

Maintaining a rigorous data lineage is a priority in Mosaico. To ensure that the history of your data remains pristine, Sequences and Topics are **immutable** once fully uploaded. This means that after the upload process is finalized, no data within that sequence can be altered.

The lifecycle of data in Mosaico follows a specific locking protocol to manage this immutability:

1.  **Creation (unlocked):** when you begin an upload (e.g., uploading a new dataset or ROS bag), Mosaico creates a new **unlocked** Sequence.
2.  **Upload (unlocked):** as you push data, the Topics are created and populated. During this phase, the Topics are also **unlocked**. This is the only window in which data can be deleted if an error occurs.
3.  **Finalization (locked):** once the client confirms that all data has been uploaded successfully, it sends a command to **lock** the Sequence.

A **locked** status signifies that the data is now permanent and immutable. An **unlocked** status implies the data is still in a transient state and can be deleted.

## Looking Ahead

We are actively working on features to expand the flexibility of Mosaico without compromising data integrity.

Future updates will introduce a **Versioning System**. This will allow you to generate new versions of existing sequences by creating a "diff" against the previous version. This approach maintains the lineage of the original data while improving storage efficiency and performance for updates.

Additionally, we will soon introduce **Services**. Through an SDK, users will be able to run custom processing logic triggered by platform events. For example, you could configure a service to automatically process images the moment a new sequence containing visual data finishes uploading.
