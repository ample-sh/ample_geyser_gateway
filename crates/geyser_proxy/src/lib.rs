use agave_geyser_plugin_interface::geyser_plugin_interface::GeyserPlugin;

mod config;
mod plugin;
mod account_coalescer;

#[unsafe(no_mangle)]
#[allow(improper_ctypes_definitions)]
/// # Safety
///
/// This function returns the Plugin pointer as trait GeyserPlugin.
pub unsafe extern "C" fn _create_plugin() -> *mut dyn GeyserPlugin {
    let plugin = plugin::AmpleGeyserPluginOuter::default();
    let plugin: Box<dyn GeyserPlugin> = Box::new(plugin);
    Box::into_raw(plugin)
}
