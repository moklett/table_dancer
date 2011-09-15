require "bundler"
# Bundler.setup
# Bundler::GemHelper.install_tasks

require "rake"
# require "yaml"

require "rspec/core/rake_task"
# require "rspec/core/version"

desc "Run all examples"
RSpec::Core::RakeTask.new(:spec) do |t|
  t.rspec_opts = %w[--color]
end

task :default => [:spec]