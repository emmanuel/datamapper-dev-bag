require "rubygems"
require "dm-core"
require "dm-migrations"
require "veritas"

require "ruby-debug"

module DataMapper
  module Model
    def relation(repository = default_repository_name)
      Veritas::Relation::Base.new(storage_names[repository], header(repository))
    end

    def header(repository = default_repository_name)
      properties(repository).map { |p| p.attribute }
    end
  end

  class Property
    def attribute
      # Veritas::Attribute.infer_type expects an instance not a Class
      # Veritas::Attribute.infer_type(primitive).new(name)
      Veritas::Attribute.coerce([name, primitive])
    end
  end

  class Query

    def relation
      model     = self.model
      relation  = model.relation
      header    = self.header
      links     = self.links

      relations = Hash[links.map { |r| [r.model, r.model.relation] }]
      relations[model] = relation

      # Only restrict if there are conditions in this Query
      relation = relation.restrict { |r| restrict_relation(r) } # if conditions.any?
      # restrict_relations_by_model(model, relations)

      # TODO: get links working as joins
      links.each { |link| relation = join_relation_with_link(relation, link) }
      # debugger

      # Only project if there is a difference in header from the base relation
      if relation.header != header
        relation = relation.project(header)
      end

      # TODO: limit, offset

      relation
    end

    def header
      fields.map { |p| p.attribute }
    end

  private

    def join_relation_with_link(relation, link)
      # links are the relationships *back* to #model, not *out from* #model:
      #   #model is link.target_model, *not* link.source_model
      link_relation = link.source_model.relation

      parent_key = link.parent_key
      child_key  = link.child_key

      # TODO: which side gets renamed needs to be based on which side has a
      # unique key name, so this needs to be conditional somehow
      if link.source_model == link.parent_model
      else
      end

      parent_key_attribute_names = parent_key.map { |p| p.name }
      child_key_attribute_names  = child_key.map { |p| p.name }
      parent_keys_needing_rename = parent_key_attribute_names - child_key_attribute_names

      if parent_keys_needing_rename.empty?
        relation.join link_relation
      else
        parent_child_key_pairs = parent_key_attribute_names.zip(child_key_attribute_names)
        key_attribute_name_mapping = Hash[parent_child_key_pairs]
        # raise parent_keys_needing_rename.inspect
        aliases = key_attribute_name_mapping.select { |pk_attr_name, fk_attr_name|
          parent_keys_needing_rename.include?(pk_attr_name)
        }

        # renamed_target = link_relation.rename(aliases)
        # relation.join(renamed_target)

        relation = relation.rename(aliases)
        relation.join(link_relation)
      end
    end

    SLUG_TO_RESTRICTION = {
      :eql   => :eq,
      :in    => :include,
      :match => :match,
      :gt    => :gt,
      :lt    => :lt,
      :gte   => :gte,
      :lte   => :lte,
    }

    # TODO: pass relations Hash into #restrict_relation, then apply
    #   restrictions to the appropriate relation based on the model of the
    #   property (or relationship) target of the condition
    def restrict_relations_by_model(model, relations)
      
    end

    def restrict_relation(relation, operation = self.conditions)
      case operation
      when Conditions::AndOperation
        tautology = Veritas::Function::Proposition::Tautology.new
        operation.operands.inject(tautology) do |conjunction, op|
          conjunction.and(restrict_relation(relation, op))
        end
      when Conditions::OrOperation
        contradiction = Veritas::Function::Proposition::Contradiction.new
        operation.operands.inject(contradiction) do |disjunction, op|
          disjunction.or(restrict_relation(relation, op))
        end
      when Conditions::NotOperation
        restrict_relation(relation, operation.operand).not
      when Conditions::NullOperation
        # This is a no-op; NullOperation matches everything
        relation
      when Conditions::AbstractComparison
        restriction_method = SLUG_TO_RESTRICTION[operation.slug]
        relation[operation.subject.name].send(restriction_method, operation.value)
      else
        raise operation.inspect
      end
    end

  end # class Query
end # module DataMapper

DataMapper::Logger.new($stdout, :default)
DataMapper.setup(:default, "sqlite::memory:")

include Veritas

class Post
  include DataMapper::Resource

  property :id,   Serial
  property :body, String

  has n, :comments
end

class Comment
  include DataMapper::Resource

  property :id,   Serial
  property :body, String

  belongs_to :post

end

DataMapper.auto_migrate!

[
  { "comments.body" => "foo" },
  # { Comment.body => "foo" },
  # { :comment => Comment.all(:body => "foo") },
].each do |format|
  query = Post.all(format).query

  p query.relation
end

