use std::collections::HashMap;

use indexmap::IndexMap;
use toml::value::Table;

use super::{
    ComponentHint, ConfigPath, Format, Process, deserialize_table, deserialize_table_wrapped,
    env_var_interpolation_enabled, interpolate_toml_table_with_secrets, loader, loader_from_paths,
};
use crate::config::{
    ComponentKey, ConfigBuilder, EnrichmentTableOuter, SinkOuter, SourceOuter, TestDefinition,
    TransformOuter,
};

#[derive(Debug)]
pub struct ConfigBuilderLoader {
    builder: ConfigBuilder,
    secrets: HashMap<String, String>,
    interpolate_env: bool,
}

impl ConfigBuilderLoader {
    /// Sets whether to interpolate environment variables in the config.
    pub const fn interpolate_env(mut self, interpolate: bool) -> Self {
        self.interpolate_env = interpolate;
        self
    }

    /// Sets the secrets map for secret interpolation.
    pub fn secrets(mut self, secrets: HashMap<String, String>) -> Self {
        self.secrets = secrets;
        self
    }

    /// Sets whether to allow empty configuration.
    pub const fn allow_empty(mut self, allow_empty: bool) -> Self {
        self.builder.allow_empty = allow_empty;
        self
    }

    /// Builds the ConfigBuilderLoader and loads configuration from the specified paths.
    pub fn load_from_paths(
        self,
        config_paths: &[ConfigPath],
    ) -> Result<ConfigBuilder, Vec<String>> {
        loader_from_paths(self, config_paths)
    }

    /// Builds the ConfigBuilderLoader and loads configuration from an input reader.
    pub fn load_from_input<R: std::io::Read>(
        self,
        input: R,
        format: Format,
    ) -> Result<ConfigBuilder, Vec<String>> {
        super::loader_from_input(self, input, format)
    }
}

impl Default for ConfigBuilderLoader {
    fn default() -> Self {
        Self {
            builder: ConfigBuilder::default(),
            secrets: HashMap::new(),
            interpolate_env: env_var_interpolation_enabled(),
        }
    }
}

impl Process for ConfigBuilderLoader {
    fn should_interpolate_env(&self) -> bool {
        self.interpolate_env
    }

    fn postprocess(&mut self, table: Table) -> Result<Table, Vec<String>> {
        if self.secrets.is_empty() {
            Ok(table)
        } else {
            interpolate_toml_table_with_secrets(&table, &self.secrets)
        }
    }

    fn merge(&mut self, table: Table, hint: Option<ComponentHint>) -> Result<(), Vec<String>> {
        match hint {
            Some(ComponentHint::Source) => {
                self.builder.sources.extend(deserialize_table_wrapped::<
                    IndexMap<ComponentKey, SourceOuter>,
                >(table, "sources")?);
            }
            Some(ComponentHint::Sink) => {
                self.builder.sinks.extend(deserialize_table_wrapped::<
                    IndexMap<ComponentKey, SinkOuter<_>>,
                >(table, "sinks")?);
            }
            Some(ComponentHint::Transform) => {
                self.builder.transforms.extend(deserialize_table_wrapped::<
                    IndexMap<ComponentKey, TransformOuter<_>>,
                >(table, "transforms")?);
            }
            Some(ComponentHint::EnrichmentTable) => {
                self.builder
                    .enrichment_tables
                    .extend(deserialize_table_wrapped::<
                        IndexMap<ComponentKey, EnrichmentTableOuter<_>>,
                    >(table, "enrichment_tables")?);
            }
            Some(ComponentHint::Test) => {
                // Tests are loaded as a name -> TestDefinition map from
                // namespaced dirs and converted to Vec<TestDefinition> at the
                // builder; the schema represents tests as a Vec, so this branch
                // skips the wrap-and-coerce path that the component hints use.
                self.builder.tests.extend(
                    deserialize_table::<IndexMap<String, TestDefinition<String>>>(table)?
                        .into_iter()
                        .map(|(_, test)| test),
                );
            }
            None => {
                self.builder.append(deserialize_table(table)?)?;
            }
        };

        Ok(())
    }
}

impl loader::Loader<ConfigBuilder> for ConfigBuilderLoader {
    fn take(self) -> ConfigBuilder {
        self.builder
    }
}
