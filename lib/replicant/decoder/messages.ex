defmodule Replicant.Decoder.Messages do
  @moduledoc """
  Decoded representations of each `pgoutput` replication message.

  See the [pgoutput message format](https://www.postgresql.org/docs/current/protocol-logicalrep-message-formats.html)
  for the wire-level semantics of each struct.

  Vendored from `WalEx.Decoder.Messages` (walex 4.8.0, MIT).
  """

  defmodule Begin do
    @moduledoc "`B` — start of a transaction; carries the final LSN, commit timestamp, and xid."
    defstruct [:final_lsn, :commit_timestamp, :xid]

    @type t :: %__MODULE__{
            final_lsn: Replicant.lsn() | nil,
            commit_timestamp: DateTime.t() | nil,
            xid: non_neg_integer() | nil
          }
  end

  defmodule Commit do
    @moduledoc "`C` — end of a transaction; carries the commit LSN and timestamp."
    defstruct [:flags, :lsn, :end_lsn, :commit_timestamp]

    @type t :: %__MODULE__{
            flags: [atom()] | nil,
            lsn: Replicant.lsn() | nil,
            end_lsn: Replicant.lsn() | nil,
            commit_timestamp: DateTime.t() | nil
          }
  end

  defmodule Origin do
    @moduledoc "`O` — origin marker emitted when a transaction has a logical replication origin."
    defstruct [:origin_commit_lsn, :name]

    @type t :: %__MODULE__{
            origin_commit_lsn: Replicant.lsn() | nil,
            name: String.t() | nil
          }
  end

  defmodule Relation do
    @moduledoc "`R` — relation (table) metadata: identifier, schema, name, replica identity, columns."
    defstruct [:id, :namespace, :name, :replica_identity, :columns]

    @type replica_identity :: :default | :nothing | :all_columns | :index

    @type t :: %__MODULE__{
            id: non_neg_integer() | nil,
            namespace: String.t() | nil,
            name: String.t() | nil,
            replica_identity: replica_identity() | nil,
            columns: [struct()] | nil
          }

    defmodule Column do
      @moduledoc "Per-column metadata within a `Relation` message."
      defstruct [:flags, :name, :type, :type_modifier]

      @type t :: %__MODULE__{
              flags: [atom()] | nil,
              name: String.t() | nil,
              type: String.t() | nil,
              type_modifier: non_neg_integer() | nil
            }
    end
  end

  defmodule Insert do
    @moduledoc "`I` — a row insert; `tuple_data` holds the new values."
    defstruct [:relation_id, :tuple_data]

    @type t :: %__MODULE__{
            relation_id: non_neg_integer() | nil,
            tuple_data: tuple() | nil
          }
  end

  defmodule Update do
    @moduledoc "`U` — a row update; carries new values and, if available, the key or old tuple."
    defstruct [:relation_id, :changed_key_tuple_data, :old_tuple_data, :tuple_data]

    @type t :: %__MODULE__{
            relation_id: non_neg_integer() | nil,
            changed_key_tuple_data: tuple() | nil,
            old_tuple_data: tuple() | nil,
            tuple_data: tuple() | nil
          }
  end

  defmodule Delete do
    @moduledoc "`D` — a row delete; carries either the key or the full old tuple, depending on `REPLICA IDENTITY`."
    defstruct [:relation_id, :changed_key_tuple_data, :old_tuple_data]

    @type t :: %__MODULE__{
            relation_id: non_neg_integer() | nil,
            changed_key_tuple_data: tuple() | nil,
            old_tuple_data: tuple() | nil
          }
  end

  defmodule Truncate do
    @moduledoc "`T` — a `TRUNCATE` of one or more relations; options indicate `CASCADE` / `RESTART IDENTITY`."
    defstruct [:number_of_relations, :options, :truncated_relations]

    @type option :: :cascade | :restart_identity

    @type t :: %__MODULE__{
            number_of_relations: non_neg_integer() | nil,
            options: [option()] | nil,
            truncated_relations: [non_neg_integer()] | nil
          }
  end

  defmodule Type do
    @moduledoc "`Y` — a type registration for a non-built-in OID."
    defstruct [:id, :namespace, :name]

    @type t :: %__MODULE__{
            id: non_neg_integer() | nil,
            namespace: String.t() | nil,
            name: String.t() | nil
          }
  end

  defmodule Unsupported do
    @moduledoc "Catch-all for replication messages Replicant does not yet decode; carries the raw binary."
    defstruct [:data]

    @type t :: %__MODULE__{data: binary() | nil}
  end
end
