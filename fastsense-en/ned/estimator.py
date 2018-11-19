import tensorflow as tf
from typing import Dict


def _parse_example(serialized_examples):
	examples = tf.parse_example(serialized_examples, features={
		"tokens": tf.VarLenFeature(tf.string),
		"possible_senses": tf.VarLenFeature(tf.int64),
		"sense": tf.FixedLenFeature([], tf.int64)
	})

	tokens_batch = examples["tokens"]
	possible_senses = examples["possible_senses"]
	sense = examples["sense"]

	tokens_indices = tokens_batch.indices
	tokens_values = tokens_batch.values
	tokens_dense_shape = tokens_batch.dense_shape

	possible_senses_indices = possible_senses.indices
	possible_senses_values = possible_senses.values
	possible_senses_dense_shape = possible_senses.dense_shape

	return (
		tokens_indices,
		tokens_values,
		tokens_dense_shape,
		possible_senses_indices,
		possible_senses_values,
		possible_senses_dense_shape,
		sense
	)


def file_input_fn(file_pattern: str, epochs: int, batch_size: int, shuffle: bool = True):
	files = tf.data.Dataset.list_files(file_pattern=file_pattern)
	if shuffle:
		files = files.shuffle(1000)
	files = files.repeat(epochs)

	dataset = files.interleave(
		lambda f: tf.data.TFRecordDataset(f, compression_type="GZIP"),
		cycle_length=32,
		block_length=4
	)
	if shuffle:
		dataset = dataset.shuffle(100000)
	dataset = dataset.batch(batch_size=batch_size)
	dataset = dataset.map(_parse_example, num_parallel_calls=8).prefetch(512)

	dataset_iterator = dataset.make_one_shot_iterator()

	t_indices, t_values, t_dense_shape, ps_indices, ps_values, ps_dense_shape, sense = dataset_iterator.get_next()

	features = {
		"tokens_indices": t_indices,
		"tokens_values": t_values,
		"tokens_dense_shape": t_dense_shape,
		"possible_senses_indices": ps_indices,
		"possible_senses_values": ps_values,
		"possible_senses_dense_shape": ps_dense_shape
	}
	labels = {
		"sense": sense
	}

	return features, labels


def _model_fn(features, labels, mode, params, config):
	number_of_senses = params["number_of_senses"]
	hash_bucket_size = params["hash_bucket_size"]
	embedding_size = params["embedding_size"]
	use_sqrtn_combiner = params["use_sqrtn_combiner"]
	clip_gradients = params["clip_gradients"]
	learning_rate = params["learning_rate"]
	decay_rate = params["decay_rate"]
	decay_steps = params["decay_steps"]
	hidden_layer_sizes = params.get("hidden_layer_sizes", [])
	dropout_keep_prob = params.get("dropout_keep_prob", 1.0)

	tf_random_seed = config.tf_random_seed if config is not None else None

	embeddings = tf.get_variable(
		name="embeddings",
		shape=[hash_bucket_size, embedding_size],
		dtype=tf.float32,
		initializer=tf.contrib.layers.xavier_initializer(seed=tf_random_seed),
		trainable=True
	)

	hidden_layer_weights = []
	hidden_layer_biases = []

	prev_layer_size = embedding_size

	for layer_index in range(len(hidden_layer_sizes)):
		hidden_layer_size = hidden_layer_sizes[layer_index]

		with tf.variable_scope("layer{:d}_".format(layer_index + 1)):
			l_weights = tf.get_variable(
				name="weights",
				shape=[prev_layer_size, hidden_layer_size],
				dtype=tf.float32,
				initializer=tf.contrib.layers.xavier_initializer(
					seed=(tf_random_seed + 10 + layer_index) if tf_random_seed is not None else None
				),
				trainable=True
			)
			l_biases = tf.get_variable(
				name="biases",
				shape=[hidden_layer_size],
				dtype=tf.float32,
				initializer=tf.zeros_initializer(),
				trainable=True
			)

			hidden_layer_weights.append(l_weights)
			hidden_layer_biases.append(l_biases)

		prev_layer_size = hidden_layer_size

	out_weights_transposed = tf.get_variable(
		name="out_weights_transposed",
		shape=[number_of_senses, prev_layer_size],
		dtype=tf.float32,
		initializer=tf.contrib.layers.xavier_initializer(seed=(tf_random_seed + 2) if tf_random_seed is not None else None),
		trainable=True
	)

	out_biases = tf.get_variable(
		name="out_biases",
		shape=[number_of_senses],
		dtype=tf.float32,
		initializer=tf.zeros_initializer(),
		trainable=True
	)

	if mode == tf.estimator.ModeKeys.PREDICT:
		tokens = tf.string_to_hash_bucket_fast(features["tokens"], num_buckets=hash_bucket_size)
		possible_senses = features["possible_senses"]

		embedded_tokens = tf.nn.embedding_lookup(embeddings, tokens)

		if use_sqrtn_combiner:
			embedded_tokens = tf.reduce_sum(embedded_tokens, axis=0) / tf.sqrt(tf.cast(tf.size(tokens), tf.float32))
		else:
			embedded_tokens = tf.reduce_mean(embedded_tokens, axis=0)

		embedded_tokens_batch = tf.expand_dims(embedded_tokens, axis=0)

		for w, b in zip(hidden_layer_weights, hidden_layer_biases):
			embedded_tokens_batch = tf.matmul(embedded_tokens_batch, w) + b
			embedded_tokens_batch = tf.nn.relu(embedded_tokens_batch)

		relevant_weights_transposed = tf.gather(out_weights_transposed, possible_senses)
		relevant_biases = tf.gather(out_biases, possible_senses)

		out_layer = tf.matmul(
			embedded_tokens_batch,
			relevant_weights_transposed,
			transpose_b=True
		) + relevant_biases

		out_layer_logits = tf.squeeze(out_layer, axis=0)

		relative_prediction = tf.argmax(out_layer_logits)
		prediction = possible_senses[relative_prediction]

		export_outputs = {
			"output": tf.estimator.export.PredictOutput(outputs={"out_layer_logits": out_layer_logits, "prediction": prediction})
		}

		return tf.estimator.EstimatorSpec(mode=mode, predictions=prediction, export_outputs=export_outputs)
	else:
		tokens_batch = tf.SparseTensor(
			indices=features["tokens_indices"],
			values=tf.string_to_hash_bucket_fast(features["tokens_values"], num_buckets=hash_bucket_size),
			dense_shape=features["tokens_dense_shape"]
		)
		possible_senses_batch = tf.SparseTensor(
			indices=features["possible_senses_indices"],
			values=features["possible_senses_values"],
			dense_shape=features["possible_senses_dense_shape"]
		)
		senses_batch = labels["sense"]

		embedded_tokens_batch = tf.nn.embedding_lookup_sparse(
			params=embeddings,
			sp_ids=tokens_batch,
			sp_weights=None,
			combiner="sqrtn" if use_sqrtn_combiner else "mean"
		)

		unique_possible_senses, relative_possible_senses_idx = tf.unique(possible_senses_batch.values)

		for w, b in zip(hidden_layer_weights, hidden_layer_biases):
			embedded_tokens_batch = tf.matmul(embedded_tokens_batch, w) + b
			embedded_tokens_batch = tf.nn.relu(embedded_tokens_batch)

		relative_senses_batch = tf.argmax(
			tf.cast(
				tf.equal(
					tf.expand_dims(senses_batch, axis=-1),
					unique_possible_senses
				),
				tf.float32
			),
			axis=1
		)

		if dropout_keep_prob < 1.0 and mode == tf.estimator.ModeKeys.TRAIN:
			embedded_tokens_batch = tf.nn.dropout(embedded_tokens_batch, dropout_keep_prob, name="dropout")

		relevant_weights_transposed = tf.gather(out_weights_transposed, unique_possible_senses)
		relevant_biases = tf.gather(out_biases, unique_possible_senses)

		out_layer = tf.matmul(
			embedded_tokens_batch,
			relevant_weights_transposed,
			transpose_b=True
		) + relevant_biases

		loss = tf.reduce_mean(
			tf.nn.sparse_softmax_cross_entropy_with_logits(
				logits=out_layer,
				labels=relative_senses_batch
			)
		)

		identity_matrix = tf.eye(tf.size(unique_possible_senses), dtype=tf.float32)
		relative_possible_senses_one_hot = tf.gather(identity_matrix, relative_possible_senses_idx)
		segment_ids = tf.cast(possible_senses_batch.indices, tf.int32)[:, 0]
		mask = tf.segment_sum(relative_possible_senses_one_hot, segment_ids)

		masked_out_layer = out_layer + (tf.reduce_max(out_layer) - tf.reduce_min(out_layer) + 1.0) * mask
		relative_predictions = tf.argmax(masked_out_layer, axis=1)

		if mode == tf.estimator.ModeKeys.TRAIN:
			global_step_var = tf.train.get_global_step()

			if decay_rate != 1.0:
				actual_learning_rate = tf.train.exponential_decay(
					learning_rate=learning_rate,
					global_step=global_step_var,
					decay_steps=decay_steps,
					decay_rate=decay_rate,
					staircase=False
				)
			else:
				actual_learning_rate = learning_rate

			optimizer = tf.train.GradientDescentOptimizer(learning_rate=actual_learning_rate)

			if clip_gradients:
				grads_and_vars = optimizer.compute_gradients(loss)
				grads = list(map(lambda x: x[0], grads_and_vars))
				variables = list(map(lambda x: x[1], grads_and_vars))
				grads, global_norm = tf.clip_by_global_norm(grads, 1)
				grads_and_vars = zip(grads, variables)
				optimizer = optimizer.apply_gradients(grads_and_vars, global_step=global_step_var)
			else:
				optimizer = optimizer.minimize(loss, global_step=global_step_var)

			correct_predictions = tf.cast(tf.equal(relative_predictions, relative_senses_batch), tf.float32)
			accuracy = tf.reduce_mean(correct_predictions)

			tf.summary.scalar("accuracy", accuracy)
			tf.summary.scalar("loss", loss)
			tf.summary.scalar("learning_rate", actual_learning_rate)

			return tf.estimator.EstimatorSpec(mode=mode, loss=loss, train_op=optimizer)
		elif mode == tf.estimator.ModeKeys.EVAL:
			predictions = tf.nn.embedding_lookup(unique_possible_senses, relative_predictions)

			accuracy = tf.metrics.accuracy(
				labels=relative_senses_batch,
				predictions=relative_predictions
			)

			mean_per_class_acc = tf.metrics.mean_per_class_accuracy(
				labels=senses_batch,
				predictions=predictions,
				num_classes=number_of_senses
			)

			eval_metric_ops = {
				"accuracy": accuracy,
				"mean_per_class_accuracy": mean_per_class_acc
			}

			return tf.estimator.EstimatorSpec(mode=mode, loss=loss, eval_metric_ops=eval_metric_ops)


class WordSenseEstimator(tf.estimator.Estimator):
	"""
	Word sense estimator.

	params has the following default values:
	{
		"hash_bucket_size": 10000000,
		"embedding_size": 10,
		"use_sqrtn_combiner": False,
		"clip_gradients": True,
		"learning_rate": 1.0,
		"decay_rate": 0.98,
		"decay_steps": 100000
	}
	"""

	def __init__(
			self,
			number_of_senses: int,
			model_dir: str = None,
			params: Dict[str, any] = None,
			config: tf.estimator.RunConfig = None
	):
		default_params = {
			"hash_bucket_size": 10000000,
			"embedding_size": 10,
			"use_sqrtn_combiner": False,
			"clip_gradients": True,
			"learning_rate": 1.0,
			"decay_rate": 0.98,
			"decay_steps": 100000
		}

		if params is not None:
			default_params.update(params)

		default_params["number_of_senses"] = number_of_senses

		super().__init__(model_fn=_model_fn, model_dir=model_dir, config=config, params=default_params)
