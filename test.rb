require "rubygems"
require "dm-core"
require "dm-migrations"
require "veritas"

DataMapper::Logger.new($stdout, :default)
DataMapper.setup(:default, "sqlite::memory:")

include Veritas

class User
  include DataMapper::Resource

  property :id,     Serial
  property :email,  String
end

DataMapper.auto_migrate!

module DataMapper
  module Model
    # TODO: rename this Model#header ?
    def attributes(repository = default_repository_name)
      properties(repository).map { |p| p.attribute }
    end

    # TODO: rename this Model#base_relation ?
    def relation(repository = default_repository_name)
      Veritas::Relation::Base.new(storage_names[repository], attributes(repository))
    end
  end

  class Property
    def attribute
      Veritas::Attribute.coerce([name, primitive])
    end
  end

  class Query
    def header
      fields.map { |p| p.attribute }
    end

    def relation
      projection = model.relation.project(fields)
      projection.restrict { |r| restrict_relation(r) }
    end

    def restrict_relation(r)
      restrict_relation_dispatch(conditions, r)
    end

    def restrict_relation_dispatch(condition, restriction)
      case condition
      when Conditions::AndOperation
        tautology = Veritas::Function::Proposition::Tautology.new
        condition.operands.inject(tautology) do |conjunction, o|
          conjunction.and(restrict_relation_dispatch(o, restriction))
        end
      when Conditions::OrOperation
        contradiction = Veritas::Function::Proposition::Contradiction.new
        condition.operands.inject(contradiction) do |disjunction, o|
          disjunction.or(restrict_relation_dispatch(o, restriction))
        end
      when Conditions::EqualToComparison
        restriction[condition.subject.name].eq(condition.value)
      else
        raise conditions.inspect
      end
    end
    
  end
end

query = User.all(:email => "foo", :id => 1).query
p query.conditions.class

p User.relation
p query.relation
