from mosaicolabs.helpers import pack_topic_resource_name, unpack_topic_full_path


def test_pack_topic_resource_name():
    sequence_name = "/test_sequence"
    topic_name = "/my/topic/name"

    assert (
        pack_topic_resource_name(sequence_name, topic_name)
        == "test_sequence/my/topic/name"
    )

    sequence_name = "test_sequence"
    topic_name = "my/topic/name"

    assert (
        pack_topic_resource_name(sequence_name, topic_name)
        == "test_sequence/my/topic/name"
    )


def test_unpack_topic_resource_name():
    topic_resrc_name = "test_sequence/my/topic/name"
    sname_tname = unpack_topic_full_path(topic_resrc_name)
    assert sname_tname is not None
    sname, tname = sname_tname
    assert sname == "test_sequence" and tname == "/my/topic/name"

    topic_resrc_name = "/test_sequence/my/topic/name"
    sname_tname = unpack_topic_full_path(topic_resrc_name)
    assert sname_tname is not None
    sname, tname = sname_tname
    assert sname == "test_sequence" and tname == "/my/topic/name"

    assert unpack_topic_full_path("not-unpacked-str") is None
    assert unpack_topic_full_path("/not-unpacked-str") is None
