use crate::MultiPartForm;
use deboa::TestResult;

#[test]
fn test_multipart_form_content_type() -> TestResult<()> {
    let form = MultiPartForm::builder()
        .field("name", "deboa")
        .field("version", "0.0.1");

    assert_eq!(
        form.content_type(),
        "multipart/form-data; boundary=".to_string() + &form.boundary()
    );

    Ok(())
}
