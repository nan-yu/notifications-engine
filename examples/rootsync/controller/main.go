package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/argoproj/notifications-engine/pkg/api"
	"github.com/argoproj/notifications-engine/pkg/controller"
	"github.com/argoproj/notifications-engine/pkg/services"

	"github.com/spf13/cobra"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/clientcmd"

	_ "k8s.io/client-go/plugin/pkg/client/auth/gcp"
)

func main() {
	var (
		clientConfig clientcmd.ClientConfig
	)
	var command = cobra.Command{
		Use: "controller",
		Run: func(c *cobra.Command, args []string) {
			// Get Kubernetes REST Config and current Namespace so we can talk to Kubernetes
			restConfig, err := clientConfig.ClientConfig()
			if err != nil {
				log.Fatalf("Failed to get Kubernetes config")
			}
			namespace, _, err := clientConfig.Namespace()
			if err != nil {
				log.Fatalf("Failed to get namespace from Kubernetes config")
			}

			// Create ConfigMap and Secret informer to access notifications configuration
			informersFactory := informers.NewSharedInformerFactoryWithOptions(
				kubernetes.NewForConfigOrDie(restConfig),
				time.Minute,
				informers.WithNamespace(namespace))
			secrets := informersFactory.Core().V1().Secrets().Informer()
			configMaps := informersFactory.Core().V1().ConfigMaps().Informer()

			// Create "Notifications" API factory that handles notifications processing
			notificationsFactory := api.NewFactory(api.Settings{
				ConfigMapName: "root-sync-notifications-cm",
				SecretName:    "root-sync-notifications-secret",
				InitGetVars: func(cfg *api.Config, configMap *v1.ConfigMap, secret *v1.Secret) (api.GetVars, error) {
					return func(obj map[string]interface{}, dest services.Destination) map[string]interface{} {
						return map[string]interface{}{"sync": obj}
					}, nil
				},
			}, namespace, secrets, configMaps)

			// Create notifications controller that handles Kubernetes resources processing
			csClient := dynamic.NewForConfigOrDie(restConfig).Resource(schema.GroupVersionResource{
				Group: "configsync.gke.io", Version: "v1beta1", Resource: "rootsyncs",
			})
			csInformer := cache.NewSharedIndexInformer(&cache.ListWatch{
				ListFunc: func(options metav1.ListOptions) (runtime.Object, error) {
					return csClient.List(context.Background(), options)
				},
				WatchFunc: func(options metav1.ListOptions) (watch.Interface, error) {
					return csClient.Watch(context.Background(), metav1.ListOptions{})
				},
			}, &unstructured.Unstructured{}, time.Minute, cache.Indexers{})
			ctrl := controller.NewController(csClient, csInformer, notificationsFactory)

			// Start informers and controller
			go informersFactory.Start(context.Background().Done())
			go csInformer.Run(context.Background().Done())
			if !cache.WaitForCacheSync(context.Background().Done(), secrets.HasSynced, configMaps.HasSynced, csInformer.HasSynced) {
				log.Fatalf("Failed to synchronize informers")
			}

			ctrl.Run(10, context.Background().Done())
		},
	}
	clientConfig = addK8SFlagsToCmd(&command)
	if err := command.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func addK8SFlagsToCmd(cmd *cobra.Command) clientcmd.ClientConfig {
	loadingRules := clientcmd.NewDefaultClientConfigLoadingRules()
	loadingRules.DefaultClientConfig = &clientcmd.DefaultClientConfig
	overrides := clientcmd.ConfigOverrides{}
	kflags := clientcmd.RecommendedConfigOverrideFlags("")
	cmd.PersistentFlags().StringVar(&loadingRules.ExplicitPath, "kubeconfig", "", "Path to a kube config. Only required if out-of-cluster")
	clientcmd.BindOverrideFlags(&overrides, cmd.PersistentFlags(), kflags)
	return clientcmd.NewInteractiveDeferredLoadingClientConfig(loadingRules, &overrides, os.Stdin)
}
