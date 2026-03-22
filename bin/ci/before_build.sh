#!/usr/bin/env sh

CTL=${BUNNY_RS_RABBITMQCTL:-"sudo rabbitmqctl"}
PLUGINS=${BUNNY_RS_RABBITMQ_PLUGINS:-"sudo rabbitmq-plugins"}

case $CTL in
        DOCKER*)
          PLUGINS="docker exec ${CTL##*:} rabbitmq-plugins"
          CTL="docker exec ${CTL##*:} rabbitmqctl";;
esac

echo "Will use rabbitmqctl at ${CTL}"
echo "Will use rabbitmq-plugins at ${PLUGINS}"

$PLUGINS enable rabbitmq_management

sleep 3

# guest:guest has full access to /
$CTL add_vhost /
$CTL add_user guest guest || true
$CTL set_user_tags guest "administrator"
$CTL set_permissions -p / guest ".*" ".*" ".*"

# test vhosts
$CTL add_vhost bunny_rs_testbed
$CTL set_permissions -p bunny_rs_testbed guest ".*" ".*" ".*"

# Reduce retention policy for faster publishing of stats
$CTL eval 'supervisor2:terminate_child(rabbit_mgmt_sup_sup, rabbit_mgmt_sup), application:set_env(rabbitmq_management,       sample_retention_policies, [{global, [{605, 1}]}, {basic, [{605, 1}]}, {detailed, [{10, 1}]}]), rabbit_mgmt_sup_sup:start_child().' || true
$CTL eval 'supervisor2:terminate_child(rabbit_mgmt_agent_sup_sup, rabbit_mgmt_agent_sup), application:set_env(rabbitmq_management_agent, sample_retention_policies, [{global, [{605, 1}]}, {basic, [{605, 1}]}, {detailed, [{10, 1}]}]), rabbit_mgmt_agent_sup_sup:start_child().' || true

$CTL set_cluster_name rabbitmq@localhost

$CTL enable_feature_flag all

$PLUGINS enable rabbitmq_stream
$PLUGINS enable rabbitmq_stream_management

true
