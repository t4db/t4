// @ts-check
import { defineConfig } from 'astro/config';
import starlight from '@astrojs/starlight';
import { visit } from 'unist-util-visit';

/**
 * Rehype plugin that prepends the configured base path to every root-relative
 * link in markdown/MDX content (i.e. href="/foo" → href="/t4/foo").
 * This lets content files use bare paths like "/page/" without knowing the
 * deployment base, which Astro does not rewrite automatically for prose links.
 */
function rehypePrependBase(base) {
	const normalised = base.replace(/\/$/, '');
	return () => (tree) => {
		visit(tree, 'element', (node) => {
			if (node.tagName === 'a' && typeof node.properties?.href === 'string') {
				const href = node.properties.href;
				if (href.startsWith('/') && !href.startsWith('//')) {
					node.properties.href = normalised + href;
				}
			}
		});
	};
}

// When deploying to GitHub Pages the CI sets SITE and BASE_PATH via
// the configure-pages action so internal links resolve correctly.
// In local dev both are unset and the defaults below are used.
const site = process.env.SITE ?? 'https://t4db.github.io';
const base = process.env.BASE_PATH ?? '/t4';
const posthogKey = process.env.PUBLIC_POSTHOG_KEY;
const posthogHost = process.env.PUBLIC_POSTHOG_HOST || 'https://eu.i.posthog.com';

const head = [
	{
		tag: 'meta',
		attrs: {
			property: 'og:description',
			content: 'An embeddable, S3-durable key-value store for Go with etcd v3 compatibility.',
		},
	},
];

if (posthogKey) {
	head.push({
		tag: 'script',
		content: [
			'!function(t,e){var o,n,p,r;e.__SV||(window.posthog=e,e._i=[],e.init=function(i,s,a){function g(t,e){var o=e.split(".");2==o.length&&(t=t[o[0]],e=o[1]),t[e]=function(){t.push([e].concat(Array.prototype.slice.call(arguments,0)))}}(p=t.createElement("script")).type="text/javascript",p.crossOrigin="anonymous",p.async=!0,p.src=s.api_host.replace(".i.posthog.com","-assets.i.posthog.com")+"/static/array.js",(r=t.getElementsByTagName("script")[0]).parentNode.insertBefore(p,r);var u=e;for(void 0!==a?u=e[a]=[]:a="posthog",u.people=u.people||[],u.toString=function(t){var e="posthog";return"posthog"!==a&&(e+="."+a),t||(e+=" (stub)"),e},u.people.toString=function(){return u.toString(1)+".people (stub)"},o="init capture register register_once register_for_session unregister unregister_for_session getFeatureFlag getFeatureFlagPayload isFeatureEnabled reloadFeatureFlags updateEarlyAccessFeatureEnrollment getEarlyAccessFeatures on onFeatureFlags onSessionId getSurveys getActiveMatchingSurveys renderSurvey canRenderSurvey getNextSurveyStep identify setPersonProperties group resetGroups setPersonPropertiesForFlags resetPersonPropertiesForFlags setGroupPropertiesForFlags resetGroupPropertiesForFlags reset get_distinct_id getGroups get_session_id get_session_replay_url alias set_config startSessionRecording stopSessionRecording sessionRecordingStarted captureException loadToolbar get_property getSessionProperty createPersonProfile opt_in_capturing opt_out_capturing has_opted_in_capturing has_opted_out_capturing clear_opt_in_out_capturing debug".split(" "),n=0;n<o.length;n++)g(u,o[n]);e._i.push([i,s,a])},e.__SV=1)}(document,window.posthog||[]);',
			`posthog.init(${JSON.stringify(posthogKey)}, { api_host: ${JSON.stringify(posthogHost)}, defaults: '2026-01-30', persistence: 'memory' });`,
		].join('\n'),
	});
}

// https://astro.build/config
export default defineConfig({
	site,
	base,
	markdown: {
		rehypePlugins: [rehypePrependBase(base)],
	},
	integrations: [
		starlight({
			title: 'T4',
			description: 'An embeddable, S3-durable key-value store for Go.',
			social: [
				{ icon: 'github', label: 'GitHub', href: 'https://github.com/t4db/t4' },
			],
			customCss: ['./src/styles/custom.css'],
			components: {
				Hero: './src/components/Hero.astro',
				Header: './src/components/Header.astro',
			},
			sidebar: [
				{ label: 'Getting Started', slug: 'getting-started' },
				{ label: 'Why T4 Exists', slug: 'why' },
				{
					label: 'Guides',
					items: [
						{ label: 'API Reference', slug: 'api' },
						{ label: 'Configuration', slug: 'configuration' },
						{ label: 'Operations', slug: 'operations' },
						{ label: 'Branches', slug: 'branches' },
						{ label: 'Backup and Restore', slug: 'backup-restore' },
						{ label: 'Security', slug: 'security' },
						{ label: 'Recipes', slug: 'recipes' },
					],
				},
				{
					label: 'Deployment',
					items: [
						{ label: 'Kubernetes', slug: 'deployment/kubernetes' },
						{ label: 'Docker Compose', slug: 'deployment/docker-compose' },
					],
				},
				{
					label: 'Reference',
					items: [
						{ label: 'Architecture', slug: 'architecture' },
						{ label: 'v1 Compatibility', slug: 'v1-compatibility' },
						{ label: 'Consistency Model', slug: 'consistency' },
						{ label: 'Failure Scenarios', slug: 'failure-scenarios' },
						{ label: 'Benchmarks', slug: 'benchmarks' },
					],
				},
				{ label: 'Migrating from etcd', slug: 'etcd-migration' },
				{ label: 'Troubleshooting', slug: 'troubleshooting' },
				{ label: 'FAQ', slug: 'faq' },
			],
			head,
		}),
	],
});
