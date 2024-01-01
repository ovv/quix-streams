from quixstreams.topic_manager import TopicConfig
from quixstreams.models.serializers import BytesSerializer, BytesDeserializer


class TestTopicManager:
    def test_topic_config(self, topic_manager_factory):
        """
        `TopicConfig` is created with expected defaults where necessary.
        """
        topic_manager = topic_manager_factory()
        topic_manager._topic_extra_config_defaults = {
            "a.config": "a_default",
            "another.config": "value",
        }
        topic_configs = topic_manager.topic_config(
            num_partitions=5, extra_config={"a.config": "woo"}
        )

        assert topic_configs.num_partitions == 5
        assert topic_configs.replication_factor == 1
        assert topic_configs.extra_config == {
            "a.config": "woo",
            "another.config": "value",
        }

    def test_topic_with_config(self, topic_manager_factory):
        """
        `Topic` is created with expected passed `TopicConfig` and added to the list
        of topics stored on the `TopicManager`.
        """
        topic_manager = topic_manager_factory()
        topic_name = "my_topic"
        extras = {"a_config": "woo"}
        topic_partitions = 5
        topic_replication = 5
        topic = topic_manager.topic(
            name=topic_name,
            config=TopicConfig(
                num_partitions=topic_partitions,
                replication_factor=topic_replication,
                extra_config=extras,
            ),
        )

        assert topic_manager.topics[topic_name] == topic

        assert topic.name == topic_name
        assert topic.config.num_partitions == topic_partitions
        assert topic.config.replication_factor == topic_replication
        assert topic.config.extra_config == extras

    def test_topic_no_config(self, topic_manager_factory):
        """
        `Topic` is created with expected passed config.
        """
        topic_manager = topic_manager_factory()
        topic_name = "my_topic"
        topic = topic_manager.topic(name=topic_name)

        assert topic.name == topic_name
        assert topic.config.num_partitions == topic_manager._topic_partitions
        assert topic.config.replication_factor == topic_manager._topic_replication

    def test_topic_no_auto_create_config(self, topic_manager_factory):
        """
        `Topic` is created with expected passed config.
        """
        topic_manager = topic_manager_factory()
        topic_name = "my_topic"
        topic = topic_manager.topic(name=topic_name, auto_create_config=False)

        assert topic.name == topic_name
        assert topic.config is None

    def test_changelog_topic(self, topic_manager_factory):
        """
        A changelog `Topic` is created with settings that match the source `Topic`
        and is added to the changelog topic list stored on the `TopicManager`.
        """

        topic_manager = topic_manager_factory()
        topic = topic_manager.topic(
            name="my_topic",
            config=topic_manager.topic_config(num_partitions=5),
        )

        suffix = "default"
        group = "my_consumer_group"
        changelog = topic_manager.changelog_topic(
            source_topic_name=topic.name,
            suffix=suffix,
            consumer_group=group,
        )

        assert topic_manager.changelog_topics[topic.name][suffix] == changelog

        assert changelog.name == topic_manager._format_changelog_name(
            group, topic.name, suffix
        )
        for attr in [
            "_key_serializer",
            "_value_serializer",
        ]:
            assert isinstance(getattr(changelog, attr), BytesSerializer)
        for attr in ["_key_deserializer", "_value_deserializer"]:
            assert isinstance(getattr(changelog, attr), BytesDeserializer)
        assert changelog.config.num_partitions == topic.config.num_partitions
        assert changelog.config.replication_factor == topic.config.replication_factor
        assert changelog.config.extra_config["cleanup.policy"] == "compact"

    def test_changelog_topic_settings_import(self, topic_manager_factory):
        """
        A changelog `Topic` only imports specified extra_configs from source `Topic`.
        """

        topic_manager = topic_manager_factory()
        topic_manager._changelog_extra_config_imports_defaults = {"import.this"}
        topic = topic_manager.topic(
            name="my_topic",
            config=topic_manager.topic_config(
                extra_config={"import.this": "different", "ignore.this": "woo"}
            ),
        )
        changelog = topic_manager.changelog_topic(
            source_topic_name=topic.name,
            suffix="default",
            consumer_group="my_consumer_group",
        )

        assert "import.this" in changelog.config.extra_config
        assert "ignore.this" not in changelog.config.extra_config

    def test_changelog_topic_source_exists_in_cluster(
        self, topic_manager_factory, admin, topic_factory
    ):
        """
        `TopicConfig` is inferred from the cluster topic metadata rather than the
        source `Topic` object if the topic already exists AND an `Admin` is provided.
        """

        topic_manager = topic_manager_factory(admin=admin)
        topic_manager._changelog_extra_config_imports_defaults = {"ignore.this"}
        topic_name, partitions = topic_factory(num_partitions=5, timeout=15)

        topic = topic_manager.topic(
            name=topic_name,
            config=topic_manager.topic_config(
                num_partitions=1,
                extra_config={"ignore.this": "not.set.on.cluster.topic.so.ignore"},
            ),
        )
        changelog = topic_manager.changelog_topic(
            source_topic_name=topic.name,
            suffix="default",
            consumer_group="my_consumer_group",
        )

        assert changelog.config.num_partitions == partitions == 5
        assert "ignore.this" not in changelog.config.extra_config
