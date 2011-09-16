module TableDancer
  module Speech
    def self.included(mod)
      mod.send(:include, Methods)
      mod.send(:extend, Methods)
    end
    
    module Methods
      def log(msg)
        return if logger.nil?
        logger.info(msg_with_time(msg))
      end

      def logger
        @logger ||= ActiveRecord::Base.logger
      end

      def say(msg, indent = 0)
        if verbose?
          puts msg_with_indention(msg_with_time(msg),indent)
        end
      end

      def resay(msg, indent=0)
        if verbose?
          jump = "\r\e[0K" # That is return to beginning of line and use the
                           # ANSI clear command "\e" or "\003"
          $stdout.flush
          print "#{jump}#{msg_with_indention(msg,indent)}"
        end
      end
      
      private

      def msg_with_time(msg)
        "#{msg} (#{Time.now.strftime('%H:%M:%S')})" 
      end
      
      def msg_with_indention(msg, indent = 0)
        "#{'  '*indent}#{msg}"
      end
      
      def verbose?
        TableDancer.verbose == true
      end
    end
  end
end