package podwatcher

import (
	"context"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/informers"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

type PodWatcher struct {
	mu     sync.Mutex
	m      map[string]*metav1.ObjectMeta
	client kubernetes.Interface
}

func New() (*PodWatcher, error) {
	config, err := rest.InClusterConfig()
	if err != nil {
		return nil, err
	}
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return nil, err
	}

	return &PodWatcher{
		m:      make(map[string]*metav1.ObjectMeta),
		client: clientset,
	}, nil
}

func (pw *PodWatcher) Run(ctx context.Context) error {
	informers := informers.NewSharedInformerFactory(pw.client, 10*time.Minute)
	podInformer := informers.Core().V1().Pods().Informer()
	podInformer.AddEventHandler(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    pw.addPod,
			UpdateFunc: pw.updatePod,
			DeleteFunc: pw.deletePod,
		},
	)
	informers.Start(ctx.Done())

	if !cache.WaitForCacheSync(ctx.Done(), podInformer.HasSynced) {
		logrus.Warn("timed out waiting for caches to sync. Starting informers")
	}
	<-ctx.Done()
	return nil
}

func namespacedName(meta *metav1.ObjectMeta) string {
	return meta.Namespace + "/" + meta.Name
}

func (pw *PodWatcher) addPod(obj interface{}) {
	pod := obj.(*corev1.Pod)
	pw.mu.Lock()
	pw.m[namespacedName(&pod.ObjectMeta)] = &pod.ObjectMeta
	pw.mu.Unlock()
}

func (pw *PodWatcher) updatePod(_, newObj interface{}) {
	pod := newObj.(*corev1.Pod)
	pw.mu.Lock()
	pw.m[namespacedName(&pod.ObjectMeta)] = &pod.ObjectMeta
	pw.mu.Unlock()
}

func (pw *PodWatcher) deletePod(obj interface{}) {
	pod := obj.(*corev1.Pod)
	pw.mu.Lock()
	delete(pw.m, namespacedName(&pod.ObjectMeta))
	pw.mu.Unlock()
}

func (pw *PodWatcher) GetObjectMeta(namespace, name string) *metav1.ObjectMeta {
	pw.mu.Lock()
	defer pw.mu.Unlock()
	return pw.m[namespace+"/"+name]
}
