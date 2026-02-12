use serde_json::Value as JsonValue;
use sqlx::sqlite::SqliteValueRef;
use sqlx::{TypeInfo, Value, ValueRef};
use time::PrimitiveDateTime;

use crate::Error;

/// Convert a SQLite value to a JSON value.
///
/// This function handles the type conversion from SQLite's native types
/// to JSON-compatible representations.
///
/// Note: BLOB values are returned as base64-encoded strings since JSON
/// has no native binary type. Boolean values are stored as INTEGER in SQLite.
pub fn to_json(value: SqliteValueRef) -> Result<JsonValue, Error> {
   if value.is_null() {
      return Ok(JsonValue::Null);
   }

   let column_type = value.type_info();

   // Handle types based on SQLite's type affinity
   let result = match column_type.name() {
      "TEXT" => {
         if let Ok(v) = value.to_owned().try_decode::<String>() {
            JsonValue::String(v)
         } else {
            JsonValue::Null
         }
      }

      "REAL" => {
         if let Ok(v) = value.to_owned().try_decode::<f64>() {
            JsonValue::from(v)
         } else {
            JsonValue::Null
         }
      }

      "INTEGER" | "NUMERIC" => {
         if let Ok(v) = value.to_owned().try_decode::<i64>() {
            JsonValue::Number(v.into())
         } else {
            JsonValue::Null
         }
      }

      "BOOLEAN" => {
         if let Ok(v) = value.to_owned().try_decode::<bool>() {
            JsonValue::Bool(v)
         } else {
            JsonValue::Null
         }
      }

      "DATE" => {
         // SQLite stores dates as TEXT in ISO 8601 format
         if let Ok(v) = value.to_owned().try_decode::<String>() {
            JsonValue::String(v)
         } else {
            JsonValue::Null
         }
      }

      "TIME" => {
         // SQLite stores time as TEXT in HH:MM:SS format
         if let Ok(v) = value.to_owned().try_decode::<String>() {
            JsonValue::String(v)
         } else {
            JsonValue::Null
         }
      }

      "DATETIME" => {
         // Try to decode as PrimitiveDateTime
         if let Ok(dt) = value.to_owned().try_decode::<PrimitiveDateTime>() {
            JsonValue::String(dt.to_string())
         } else if let Ok(v) = value.to_owned().try_decode::<String>() {
            // Fall back to string representation
            JsonValue::String(v)
         } else {
            JsonValue::Null
         }
      }

      "BLOB" => {
         if let Ok(blob) = value.to_owned().try_decode::<Vec<u8>>() {
            // Encode binary data as base64 for JSON serialization
            JsonValue::String(base64_encode(&blob))
         } else {
            JsonValue::Null
         }
      }

      "NULL" => JsonValue::Null,

      _ => {
         // For unknown types, try to decode as text
         if let Ok(text) = value.to_owned().try_decode::<String>() {
            JsonValue::String(text)
         } else {
            return Err(Error::UnsupportedDatatype(format!(
               "Unknown SQLite type: {}",
               column_type.name()
            )));
         }
      }
   };

   Ok(result)
}

/// Base64 encode binary data for JSON serialization.
///
/// SQLite BLOB columns are encoded as base64 strings when serialized to JSON,
/// as JSON does not have a native binary type.
fn base64_encode(data: &[u8]) -> String {
   use base64::Engine;
   base64::engine::general_purpose::STANDARD.encode(data)
}

#[cfg(test)]
mod tests {
   use super::*;

   #[test]
   fn test_base64_encode() {
      assert_eq!(base64_encode(b"hello"), "aGVsbG8=");
      assert_eq!(base64_encode(&[1, 2, 3, 4, 5]), "AQIDBAU=");
      assert_eq!(base64_encode(&[]), "");
   }

   #[test]
   fn test_base64_encode_binary() {
      // Test with binary data including null bytes
      assert_eq!(base64_encode(&[0, 0, 0]), "AAAA");
      assert_eq!(base64_encode(&[255, 255, 255]), "////");
   }

   #[test]
   fn test_base64_encode_large() {
      // Test with larger binary data
      let data: Vec<u8> = (0..255).collect();
      let encoded = base64_encode(&data);
      assert!(!encoded.is_empty());
      // Verify it's valid base64 (only contains valid chars)
      assert!(
         encoded
            .chars()
            .all(|c| c.is_alphanumeric() || c == '+' || c == '/' || c == '=')
      );
   }
}
