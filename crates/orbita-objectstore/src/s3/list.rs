//! Parsing the one XML response the store reads: `ListObjectsV2`.

use crate::s3::time::parse_iso8601_millis;
use crate::{ETag, ObjectError, ObjectMeta, ObjectResult};
use quick_xml::events::Event;
use quick_xml::Reader;

/// One page of a listing.
#[derive(Debug, Default)]
pub(crate) struct ListPage {
    pub objects: Vec<ObjectMeta>,
    /// Present when the listing is truncated; feed it back as
    /// `continuation-token` to fetch the next page.
    pub next_continuation_token: Option<String>,
}

/// Parses a `ListBucketResult` document.
///
/// Only the fields the [`ObjectStore`](crate::ObjectStore) contract surfaces
/// are read: key, size, entity tag, and last-modified time per object, plus
/// the pagination markers. Everything else in the document is skipped without
/// complaint, because S3-compatible servers disagree about the optional
/// elements.
///
/// `LastModified` is one such optional element: a server that omits it, or
/// emits a time this crate cannot parse, yields `None` for that object's
/// [`ObjectMeta::last_modified`](crate::ObjectMeta::last_modified) rather than
/// a guessed instant, which the sweep is required to treat as "do not touch".
pub(crate) fn parse_list_page(body: &[u8]) -> ObjectResult<ListPage> {
    let mut reader = Reader::from_reader(body);
    let mut buf = Vec::new();

    let mut page = ListPage::default();
    let mut in_contents = false;
    let mut current_tag: Option<String> = None;
    let mut key = String::new();
    let mut size = String::new();
    let mut etag = String::new();
    let mut last_modified = String::new();
    let mut truncated = false;
    let mut token = String::new();

    loop {
        match reader.read_event_into(&mut buf) {
            Ok(Event::Start(start)) => {
                let name = String::from_utf8_lossy(start.name().as_ref()).to_string();
                if name == "Contents" {
                    in_contents = true;
                    key.clear();
                    size.clear();
                    etag.clear();
                    last_modified.clear();
                } else {
                    current_tag = Some(name);
                }
            }
            Ok(Event::Text(text)) => {
                let value = text
                    .xml_content()
                    .map_err(|e| ObjectError::Other(format!("malformed list response: {e}")))?;
                match current_tag.as_deref() {
                    Some("Key") if in_contents => key.push_str(&value),
                    Some("Size") if in_contents => size.push_str(&value),
                    Some("ETag") if in_contents => etag.push_str(&value),
                    Some("LastModified") if in_contents => last_modified.push_str(&value),
                    Some("IsTruncated") => truncated = value.trim() == "true",
                    Some("NextContinuationToken") => token.push_str(&value),
                    _ => {}
                }
            }
            // quick-xml surfaces `&amp;` and friends as their own events
            // rather than folding them into the surrounding text, and keys
            // are exactly where they show up.
            Ok(Event::GeneralRef(reference)) => {
                let resolved = resolve_reference(&reference)?;
                match current_tag.as_deref() {
                    Some("Key") if in_contents => key.push(resolved),
                    Some("ETag") if in_contents => etag.push(resolved),
                    Some("NextContinuationToken") => token.push(resolved),
                    _ => {}
                }
            }
            Ok(Event::End(end)) => {
                if end.name().as_ref() == b"Contents" {
                    in_contents = false;
                    let size = size.trim().parse::<u64>().map_err(|_| {
                        ObjectError::Other(format!("non-numeric size {size:?} for key {key:?}"))
                    })?;
                    page.objects.push(ObjectMeta {
                        key: std::mem::take(&mut key),
                        size,
                        etag: ETag(std::mem::take(&mut etag)),
                        last_modified: parse_iso8601_millis(&last_modified),
                    });
                    last_modified.clear();
                }
                current_tag = None;
            }
            Ok(Event::Eof) => break,
            Ok(_) => {}
            Err(e) => {
                return Err(ObjectError::Other(format!("malformed list response: {e}")));
            }
        }
        buf.clear();
    }

    if truncated {
        if token.is_empty() {
            // A truncated listing with no way to continue would silently drop
            // objects, which for a sweep means deleting things still in use.
            return Err(ObjectError::Other(
                "list response is truncated but carries no continuation token".to_string(),
            ));
        }
        page.next_continuation_token = Some(token);
    }
    Ok(page)
}

/// Resolves one entity reference: the five the XML specification predefines,
/// plus numeric character references. S3 emits nothing else.
fn resolve_reference(reference: &quick_xml::events::BytesRef<'_>) -> ObjectResult<char> {
    if let Some(c) = reference
        .resolve_char_ref()
        .map_err(|e| ObjectError::Other(format!("malformed list response: {e}")))?
    {
        return Ok(c);
    }
    let name = reference
        .decode()
        .map_err(|e| ObjectError::Other(format!("malformed list response: {e}")))?;
    match name.as_ref() {
        "amp" => Ok('&'),
        "lt" => Ok('<'),
        "gt" => Ok('>'),
        "quot" => Ok('"'),
        "apos" => Ok('\''),
        other => Err(ObjectError::Other(format!(
            "list response uses undefined entity &{other};"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_listing_parses_keys_sizes_and_tags() {
        let body = br#"<?xml version="1.0" encoding="UTF-8"?>
<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
  <Name>bucket</Name><Prefix>p/</Prefix><KeyCount>2</KeyCount>
  <IsTruncated>false</IsTruncated>
  <Contents><Key>p/a</Key><LastModified>2026-01-01T00:00:00.000Z</LastModified>
    <ETag>&quot;abc&quot;</ETag><Size>3</Size><StorageClass>STANDARD</StorageClass></Contents>
  <Contents><Key>p/b&amp;c</Key><ETag>&quot;def&quot;</ETag><Size>10</Size></Contents>
</ListBucketResult>"#;
        let page = parse_list_page(body).expect("parses");
        assert!(page.next_continuation_token.is_none());
        assert_eq!(
            page.objects,
            vec![
                ObjectMeta {
                    key: "p/a".to_string(),
                    size: 3,
                    etag: ETag("\"abc\"".to_string()),
                    // Wired through from the entry's LastModified.
                    last_modified: Some(1_767_225_600_000),
                },
                ObjectMeta {
                    key: "p/b&c".to_string(),
                    size: 10,
                    etag: ETag("\"def\"".to_string()),
                    // This entry carries no LastModified, so the sweep is told
                    // to leave it alone rather than handed a zero.
                    last_modified: None,
                },
            ]
        );
    }

    #[test]
    fn a_truncated_listing_surfaces_its_token() {
        let body = br#"<ListBucketResult>
  <IsTruncated>true</IsTruncated>
  <NextContinuationToken>tok==</NextContinuationToken>
  <Contents><Key>k</Key><ETag>"e"</ETag><Size>1</Size></Contents>
</ListBucketResult>"#;
        let page = parse_list_page(body).expect("parses");
        assert_eq!(page.next_continuation_token.as_deref(), Some("tok=="));
    }

    #[test]
    fn a_truncated_listing_without_a_token_is_an_error() {
        let body = br#"<ListBucketResult><IsTruncated>true</IsTruncated></ListBucketResult>"#;
        assert!(parse_list_page(body).is_err());
    }
}
