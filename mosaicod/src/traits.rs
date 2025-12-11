/// Used to write asyncrously data to a path (tipically over network)
pub trait AsyncWriteToPath {
    /// Writes the provided buffer to the specified path.
    ///
    /// Returns the number of bytes written on success.
    fn write_to_path(
        &self,
        path: impl AsRef<std::path::Path>,
        buf: impl Into<bytes::Bytes>,
    ) -> impl Future<Output = std::io::Result<()>>;
}

/// A trait defining an interface for creating a flattened or "squashed" iterator
/// over a nested data structure.
///
/// This trait is used to recursively traverse a hierarchy and yield entries where
/// the entire path to the leaf value is compressed into a single string key.
pub trait SquashedIterator {
    /// The type of the raw value found at the leaf nodes of the nested structure.
    ///
    /// Implementers should define this as the final data type (e.g., `i32`, `String`, or a custom struct).
    type Value;

    /// The concrete iterator type that yields the squashed entries.
    ///
    /// **Crucially, the associated item type of this iterator must be a tuple `(String, Self::Item)`.**
    /// The `String` component represents the full, squashed key identifier (e.g., `"config.server.port"`),
    /// and the `Self::Item` component is the corresponding leaf value.
    type Iter: Iterator<Item = (String, Self::Value)>;

    /// Returns an iterator that yields the single, squashed key identifier (e.g. "a.b.c." or "pose.x") and associated value
    /// for every leaf node in the structure.
    fn squashed_iter(&self) -> Self::Iter;
}

/// A trait for converting a type into its **file extension** representation.
///
/// This is typically implemented for structs that represent file formats or
/// other entities that are conventionally identified by a file extension string.
///
/// Implementors should ensure the returned string is a valid file extension,
/// usually without the leading dot (`.`).
///
/// # Example
///
/// ```
/// use mosaicod::traits::AsExtension;
///
/// // A hypothetical struct representing a JPEG file
/// struct JpegFile;
///
/// impl AsExtension for JpegFile {
///     fn as_extension(&self) -> String {
///         "jpg".to_string()
///     }
/// }
///
/// assert_eq!(JpegFile.as_extension(), "jpg");
/// ```
pub trait AsExtension {
    /// Returns the file extension string associated with this type.
    ///
    /// The returned string should **not** include the leading dot (`.`).
    ///
    /// # Returns
    ///
    /// A `String` representing the file extension (e.g., `"png"`, `"tar"`, `"json"`).
    fn as_extension(&self) -> String;
}
