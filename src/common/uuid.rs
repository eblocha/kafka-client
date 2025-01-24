use extend::ext;
use uuid::Uuid;

#[ext]
pub impl Uuid {
    /// Convert a uuid to [`None`] if the value is nil
    fn as_optional(&self) -> Option<Uuid> {
        if self.is_nil() {
            None
        } else {
            Some(self.clone())
        }
    }
}
