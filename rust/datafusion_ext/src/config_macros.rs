use datafusion_common::{DataFusionError, Result};

pub trait ConfigParseable: Sized {
    fn parse_config(value: &str, key: &str) -> Result<Self>;
}

pub trait ConfigValueFormat {
    fn format_value(&self) -> String;
}

pub fn parse_value<T: ConfigParseable>(value: &str, key: &str) -> Result<T> {
    T::parse_config(value, key)
}

pub fn format_value<T: ConfigValueFormat + ?Sized>(value: &T) -> String {
    value.format_value()
}

impl ConfigParseable for bool {
    fn parse_config(value: &str, key: &str) -> Result<Self> {
        let normalized = value.trim().to_ascii_lowercase();
        match normalized.as_str() {
            "true" | "t" | "1" | "yes" | "y" => Ok(true),
            "false" | "f" | "0" | "no" | "n" => Ok(false),
            _ => Err(DataFusionError::Plan(format!(
                "Invalid boolean for {key}: {value}"
            ))),
        }
    }
}

impl ConfigParseable for i32 {
    fn parse_config(value: &str, key: &str) -> Result<Self> {
        value.trim().parse::<i32>().map_err(|err| {
            DataFusionError::Plan(format!("Invalid integer for {key}: {value} ({err})"))
        })
    }
}

impl ConfigParseable for usize {
    fn parse_config(value: &str, key: &str) -> Result<Self> {
        value.trim().parse::<usize>().map_err(|err| {
            DataFusionError::Plan(format!("Invalid usize for {key}: {value} ({err})"))
        })
    }
}

impl ConfigParseable for u64 {
    fn parse_config(value: &str, key: &str) -> Result<Self> {
        value
            .trim()
            .parse::<u64>()
            .map_err(|err| DataFusionError::Plan(format!("Invalid u64 for {key}: {value} ({err})")))
    }
}

impl ConfigParseable for String {
    fn parse_config(value: &str, _key: &str) -> Result<Self> {
        Ok(value.trim().to_string())
    }
}

impl<T: ConfigParseable> ConfigParseable for Option<T> {
    fn parse_config(value: &str, key: &str) -> Result<Self> {
        let trimmed = value.trim();
        if trimmed.is_empty()
            || trimmed.eq_ignore_ascii_case("none")
            || trimmed.eq_ignore_ascii_case("null")
        {
            return Ok(None);
        }
        T::parse_config(trimmed, key).map(Some)
    }
}

impl ConfigValueFormat for bool {
    fn format_value(&self) -> String {
        self.to_string()
    }
}

impl ConfigValueFormat for i32 {
    fn format_value(&self) -> String {
        self.to_string()
    }
}

impl ConfigValueFormat for usize {
    fn format_value(&self) -> String {
        self.to_string()
    }
}

impl ConfigValueFormat for u64 {
    fn format_value(&self) -> String {
        self.to_string()
    }
}

impl ConfigValueFormat for String {
    fn format_value(&self) -> String {
        self.clone()
    }
}

impl<T: ConfigValueFormat> ConfigValueFormat for Option<T> {
    fn format_value(&self) -> String {
        self.as_ref()
            .map(ConfigValueFormat::format_value)
            .unwrap_or_default()
    }
}

#[macro_export]
macro_rules! impl_extension_options {
    (
        $config_type:ty,
        prefix = $prefix:expr,
        unknown_key = $unknown_key:expr,
        fields = [
            $(($field:ident, $field_type:ty, $description:expr)),* $(,)?
        ]
    ) => {
        impl datafusion_common::config::ExtensionOptions for $config_type {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }

            fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
                self
            }

            fn cloned(&self) -> Box<dyn datafusion_common::config::ExtensionOptions> {
                Box::new(self.clone())
            }

            fn set(&mut self, key: &str, value: &str) -> datafusion_common::Result<()> {
                let prefix = $prefix;
                let key = key
                    .strip_prefix(&format!("{prefix}."))
                    .unwrap_or(key);
                match key {
                    $(
                        stringify!($field) => {
                            self.$field = $crate::config_macros::parse_value::<$field_type>(
                                value,
                                key,
                            )?;
                        }
                    )*
                    _ => {
                        return Err(datafusion_common::DataFusionError::Plan(format!(
                            $unknown_key,
                            key = key
                        )));
                    }
                }
                Ok(())
            }

            fn entries(&self) -> Vec<datafusion_common::config::ConfigEntry> {
                let prefix = $prefix;
                vec![
                    $(
                        datafusion_common::config::ConfigEntry {
                            key: format!("{prefix}.{}", stringify!($field)),
                            value: Some(
                                $crate::config_macros::format_value(&self.$field)
                            ),
                            description: $description,
                        }
                    ),*
                ]
            }
        }
    };
}
