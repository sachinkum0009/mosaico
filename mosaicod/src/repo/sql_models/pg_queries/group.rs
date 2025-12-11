use std::collections::HashMap;

use crate::{
    repo::{self, sql_models},
    types,
};

pub async fn sequences_group_from_topics(
    exe: &mut impl repo::AsExec,
    topics: Vec<sql_models::TopicRecord>,
) -> Result<Vec<types::SequenceTopicGroup>, repo::Error> {
    let mut ret: HashMap<i32, types::SequenceTopicGroup> = HashMap::new();

    for topic in topics {
        let group = ret.get_mut(&topic.sequence_id);
        if let Some((_, topics)) = group {
            topics.push(types::TopicResourceLocator::from(topic.topic_name));
        } else {
            let seq = repo::sequence_find_by_id(exe, topic.sequence_id).await?;
            ret.insert(
                seq.sequence_id,
                (
                    types::SequenceResourceLocator::from(seq.sequence_name),
                    vec![types::TopicResourceLocator::from(topic.topic_name)],
                ),
            );
        }
    }

    Ok(ret.into_values().collect())
}
