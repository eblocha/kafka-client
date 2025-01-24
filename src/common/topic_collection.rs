use uuid::Uuid;

#[derive(Debug, Clone)]
pub enum TopicCollection {
    Ids(Vec<Uuid>),
    Names(Vec<String>),
}
