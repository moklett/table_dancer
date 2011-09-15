module TableDancer
  module Speech
    def self.included(mod)
      mod.send(:include, Methods)
      mod.send(:extend, Methods)
    end
    
    module Methods
      def say(msg)
        TableDancer.say(msg)
      end
  
      def log(msg)
        TableDancer.log(msg)
      end
  
      def resay(msg)
        TableDancer.resay(msg)
      end
    end
  end
end